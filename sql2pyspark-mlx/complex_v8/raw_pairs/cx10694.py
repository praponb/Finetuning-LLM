from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), F.col("id").desc())

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.abs(F.col("kwh") - 52.4).alias("kwh_distance"),
    )
)

latest_per_key = (
    valid
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .cube("quality", "site_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("kwh_distance").alias("min_kwh_distance"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.sum(F.when(F.col("quality") != "BAD", 1).otherwise(0)).alias("n_quality_bad"),
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
