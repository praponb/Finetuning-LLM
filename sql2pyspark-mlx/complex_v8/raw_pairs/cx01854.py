from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh_ties = Window.partitionBy("quality").orderBy("kwh")
w_quality = Window.partitionBy("quality")
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_quality_by_kwh_full = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 235.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

with_trends = (
    filtered_readings
    .select(
        "*",
        F.rank().over(w_quality_by_kwh_ties).alias("kwh_rank"),
        F.max("kwh").over(w_quality).alias("quality_max_kwh"),
        F.lag("read_at").over(w_quality_by_kwh).alias("prev_read_at"),
        (F.col("kwh") - F.lag("kwh").over(w_quality_by_kwh)).alias("kwh_change"),
        F.first("kwh").over(w_quality_by_kwh_full).alias("first_kwh"),
    )
)

quality_stats = (
    with_trends
    .groupBy("quality")
    .agg(F.max("first_kwh").alias("max_first_kwh"))
)

outliers = (
    with_trends.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.first_kwh") == F.col("g.max_first_kwh"))
    .select(
        "s.*",
        "g.max_first_kwh",
        F.round(F.col("s.first_kwh") - F.col("g.max_first_kwh"), 2).alias("first_kwh_minus_max"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    outliers.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

result = (
    with_stats
    .groupingSets(
        [["site_id", "kwh_rank", "quality"], ["site_id"], ["kwh_rank"], []],
        "site_id", "kwh_rank", "quality",
    )
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.avg("first_kwh"), 2).alias("avg_first_kwh"),
        F.round(F.stddev("kwh_change"), 2).alias("stddev_kwh_change"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("kwh_rank").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
