from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc_nulls_first())
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy("read_at")

recent = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(10, 12))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_windows = (
    recent
    .withColumn("read_at_cume_dist", F.round(F.cume_dist().over(w_quality_site_id_by_read_at_t), 3))
    .withColumn("read_at_percent_rank", F.round(F.percent_rank().over(w_quality_by_read_at_ties), 3))
)

grouped = (
    with_windows
    .filter(F.col("read_at_cume_dist").between(0.3, 0.8))
    .groupBy("site_id", F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.count("*").alias("row_count"),
        F.max("kwh").alias("max_kwh"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.round(F.sum("read_at_cume_dist") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("read_at_cume_dist_to_kwh_ratio"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.sum("read_at_percent_rank").alias("sum_read_at_percent_rank_check"),
    )
    .filter(F.col("sum_read_at_percent_rank_check") > 0.2)
    .drop("sum_read_at_percent_rank_check")
)

qualified = (
    grouped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "EST") & (F.col("m.kwh") < 116.8), "left_anti")
)

result = (
    qualified
    .select(
        "site_id",
        "read_at_day",
        "row_count",
        "distinct_qualitys",
        "read_at_cume_dist_to_kwh_ratio",
        "quality_list",
        F.least(F.col("max_kwh"), F.col("distinct_qualitys")).alias("least_max_kwh_distinct_qualitys"),
        F.abs(F.col("row_count") - 9).alias("row_count_distance"),
    )
    .orderBy("row_count_distance", "site_id", "read_at_day")
    .limit(10)
)
