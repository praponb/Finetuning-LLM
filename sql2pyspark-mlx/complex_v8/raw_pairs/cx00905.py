from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("kwh_tier").orderBy("read_at", "id")
w_read_at_month_quality_by_rea = Window.partitionBy(F.month("read_at"), "quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_kwh_tier_site_id_by_first_re = Window.partitionBy("kwh_tier", "site_id").orderBy(F.col("first_reading_read_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_read_at_month_quality_by_rea_2 = Window.partitionBy(F.month("read_at"), "quality").orderBy("read_at", "id")
w_kwh_tier_site_id_by_kwh = Window.partitionBy("kwh_tier", "site_id").orderBy("kwh", "id")

filtered = (
    df_meter_readings
    .filter((F.col("kwh").between(24.5, 116.8)) & (F.col("site_id") >= 6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 109.0, "A").when(F.col("kwh") >= 37.0, "B").otherwise("C").alias("kwh_tier"),
    )
)

unique_rows = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

summarized_rows = (
    unique_rows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

windowed = (
    summarized_rows
    .select(
        "*",
        F.last("first_reading_read_at", ignorenulls=True).over(w_read_at_month_quality_by_rea).alias("last_known_first_reading_read_at"),
        F.round(F.avg("voltage").over(w_kwh_tier_site_id_by_first_re), 2).alias("voltage_avg_3"),
        F.lag("first_reading_read_at").over(w_read_at_month_quality_by_rea_2).alias("prev_first_reading_read_at"),
        F.round(F.percent_rank().over(w_kwh_tier_site_id_by_kwh), 3).alias("kwh_percent_rank"),
    )
)

result = (
    windowed
    .cube("kwh_tier", "quality")
    .agg(
        F.grouping("kwh_tier").alias("is_total_kwh_tier"),
        F.round(100 * F.count(F.when(F.col("quality") == "EST", 1)) / F.count("*"), 1).alias("pct_quality_est"),
        F.min("voltage").alias("min_voltage"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("n_quality_ok"),
    )
    .select(
        F.coalesce(F.col("kwh_tier"), F.lit("ALL")).alias("kwh_tier"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "is_total_kwh_tier",
        "pct_quality_est",
        "min_voltage",
        "n_quality_ok",
    )
    .orderBy(F.col("kwh_tier").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
