from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_minus_voltage = Window.orderBy(F.col("kwh_minus_voltage").desc(), "id")
w_site_id_by_read_at_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_quality_by_voltage = Window.partitionBy("quality").orderBy("voltage", "id")
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_quality = Window.partitionBy("quality")
w_n_rows_n_quality_est_by_n_ro = Window.partitionBy("n_rows", "n_quality_est").orderBy(F.col("n_rows").asc_nulls_last(), "first_known_read_at_month", "quality", "next_voltage_tier")
w_n_rows_n_quality_est_by_n_ro_2 = Window.partitionBy("n_rows", "n_quality_est").orderBy(F.col("n_rows").asc_nulls_last(), "first_known_read_at_month", "quality", "next_voltage_tier").rowsBetween(-6, -1)

recent_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

ranked = (
    recent_readings
    .select(
        "*",
        F.lag("read_at", 2).over(w_all_by_kwh_minus_voltage).alias("prev2_read_at"),
        F.min("kwh_minus_voltage").over(w_site_id_by_read_at_running).alias("min_kwh_minus_voltage_so_far"),
        F.lead("voltage").over(w_site_id_by_read_at).alias("next_voltage"),
        F.round(F.percent_rank().over(w_quality_by_voltage), 3).alias("voltage_percent_rank"),
        F.first("read_at", ignorenulls=True).over(w_site_id_by_read_at_running).alias("first_known_read_at"),
        (F.col("greatest_kwh_voltage") - F.lag("greatest_kwh_voltage").over(w_quality_by_voltage)).alias("greatest_kwh_voltage_change"),
        F.round(F.percent_rank().over(w_site_id_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.rank().over(w_all_by_kwh_minus_voltage).alias("kwh_minus_voltage_rank"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
    )
)

totals = (
    ranked
    .filter(F.col("site_id").isNotNull())
    .groupBy(
        F.date_trunc("month", "first_known_read_at").alias("first_known_read_at_month"),
        "quality",
        F.when(F.col("next_voltage") >= 233.1, "large").when(F.col("next_voltage") >= 220.2, "medium").otherwise("small").alias("next_voltage_tier"),
    )
    .agg(
        F.round(F.avg("rows_in_quality"), 2).alias("avg_rows_in_quality"),
        F.count(F.when(F.col("quality") == "EST", 1)).alias("n_quality_est"),
        F.count("*").alias("n_rows"),
        F.min("prev2_read_at").alias("first_prev2_read_at"),
    )
)

windowed = (
    totals
    .withColumn("prev_n_quality_est", F.lag("n_quality_est").over(w_n_rows_n_quality_est_by_n_ro))
    .withColumn("n_rows_avg_6_prior", F.round(F.avg("n_rows").over(w_n_rows_n_quality_est_by_n_ro_2), 2))
)

result = (
    windowed
    .select(
        "first_known_read_at_month",
        "next_voltage_tier",
        "avg_rows_in_quality",
        "n_rows",
        "first_prev2_read_at",
        "prev_n_quality_est",
    )
)
