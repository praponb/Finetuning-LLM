from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id")
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy("read_at")
w_quality_by_kwh_running = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_site_id_by_read_at_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_kwh_full = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")

filtered_readings = (
    df_meter_readings
    .filter(F.col("site_id") <= 5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

windowed = (
    filtered_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "read_at_date",
        F.datediff(F.col("read_at_date"), F.lag("read_at_date").over(w_quality_by_read_at)).alias("days_since_prev_read_at_date"),
        F.round(F.percent_rank().over(w_site_id_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.lag("read_at", 2).over(w_site_id_by_kwh).alias("prev2_read_at"),
        F.dense_rank().over(w_quality_by_read_at_ties).alias("read_at_dense_rank"),
        F.first("voltage", ignorenulls=True).over(w_quality_by_kwh_running).alias("first_known_voltage"),
        F.row_number().over(w_quality_by_kwh).alias("rn"),
        F.dense_rank().over(w_site_id_by_kwh).alias("kwh_dense_rank"),
        F.sum("kwh").over(w_site_id_by_read_at_running).alias("running_kwh"),
        F.first("read_at").over(w_quality_by_kwh_full).alias("first_read_at"),
        F.lag("read_at_date", 3).over(w_quality_by_read_at).alias("prev3_read_at_date"),
        F.lead("read_at", 2).over(w_site_id_by_kwh).alias("next2_read_at"),
        F.last("read_at", ignorenulls=True).over(w_site_id_by_read_at_running).alias("last_known_read_at"),
        F.ntile(3).over(w_quality_by_kwh).alias("kwh_tercile"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
    )
)

grouped = (
    windowed
    .groupBy(
        F.when(F.col("read_at_percent_rank") >= 0.8, "HIGH").when(F.col("read_at_percent_rank") >= 0.4, "MEDIUM").otherwise("LOW").alias("read_at_percent_rank_tier"),
        F.year("read_at").alias("read_at_year"),
        F.year("read_at_date").alias("read_at_date_year"),
    )
    .agg(
        F.percentile_approx("first_known_voltage", 0.5).alias("median_first_known_voltage"),
        F.round(100 * F.count(F.when(F.col("quality") == "BAD", 1)) / F.count("*"), 1).alias("pct_quality_bad"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.max("running_kwh").alias("max_running_kwh"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "read_at_percent_rank_tier",
        "read_at_year",
        "read_at_date_year",
        "median_first_known_voltage",
        "pct_quality_bad",
        "n_rows",
        "quality_list",
        "max_running_kwh",
    )
    .orderBy(F.col("pct_quality_bad").asc())
)
