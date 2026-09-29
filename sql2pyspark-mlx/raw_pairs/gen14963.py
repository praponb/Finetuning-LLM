from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "v"),
    )
)

base = (
    exploded
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("version"))))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "key",
        "v",
        F.concat("host", F.lit("/"), "level").alias("host_level"),
    )
)

base_app_logs = (
    base
    .filter(F.col("key").isNotNull() & (F.col("log_ts") >= "2025-08-01"))
    .select(
        "id",
        "key",
        "v",
        F.when(F.col("key") == "version", 1).otherwise(0).alias("is_version"),
    )
)

with_windows = (
    base_app_logs
    .select("id", "v", F.sum("is_version").over(w).alias("key_sum_is_version"))
)

filtered = (
    with_windows
    .filter(F.col("key_sum_is_version") != 232)
    .withColumn("key_sum_is_version_num", F.col("key_sum_is_version").cast("string"))
)

filtered_app_logs = (
    filtered
    .filter(F.col("key_sum_is_version").isNotNull())
    .select(
        "*",
        F.col("key_sum_is_version").cast("double").alias("key_sum_is_version_num_2"),
    )
)

result = (
    filtered_app_logs
    .select(
        "id",
        "v",
        "key_sum_is_version",
        "key_sum_is_version_num",
        "key_sum_is_version_num_2",
    )
    .orderBy(F.col("key_sum_is_version_num_2").desc(), "id")
)
