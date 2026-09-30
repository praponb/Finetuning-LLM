from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_last_read_at_moving_3 = Window.orderBy("last_read_at", "quality").rowsBetween(-3, Window.currentRow)
w_row_count_quality = Window.partitionBy("row_count", "quality")
w_quality_by_kwh_to_voltage_ra = Window.partitionBy("quality").orderBy("kwh_to_voltage_ratio", "quality")
w_row_count = Window.partitionBy("row_count")

cleaned = (
    df_meter_readings
    .filter(F.col("kwh") >= 20.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

stats = (
    cleaned
    .filter(F.col("site_id").isNotNull())
    .groupBy("quality")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count("*").alias("row_count"),
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.round(F.sum("kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("kwh_to_voltage_ratio"),
        F.min("read_at").alias("first_read_at"),
        F.max("read_at").alias("last_read_at"),
    )
    .filter(F.col("row_count") >= 2)
)

screened = (
    stats
    .crossJoin(stats.agg(F.avg("median_kwh").alias("overall_avg_median_kwh")))
    .filter(F.col("median_kwh") > F.col("overall_avg_median_kwh"))
    .drop("overall_avg_median_kwh")
)

with_windows = (
    screened
    .select(
        "*",
        F.sum("row_count").over(w_all_by_last_read_at_moving_3).alias("row_count_sum_4"),
        F.count(F.lit(1)).over(w_row_count_quality).alias("rows_in_row_count_quality"),
        F.min("kwh_to_voltage_ratio").over(w_row_count_quality).alias("row_count_quality_min_kwh_to_voltage_ratio"),
        (F.col("kwh_to_voltage_ratio") - F.lag("kwh_to_voltage_ratio").over(w_quality_by_kwh_to_voltage_ra)).alias("kwh_to_voltage_ratio_change"),
    )
)

metrics = (
    with_windows
    .withColumn("distinct_quality_in_row_count", F.size(F.collect_set("quality").over(w_row_count)))
    .withColumn("rows_in_row_count", F.count(F.lit(1)).over(w_row_count))
)

result = (
    metrics
    .groupBy("first_read_at")
    .pivot("quality", ["BAD", "OK", "EST"])
    .agg(F.avg("kwh_to_voltage_ratio"))
)
