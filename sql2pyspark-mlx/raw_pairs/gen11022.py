from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("service_length").desc(), "id", "key")

exploded = df_app_logs.select("id", "service", "level", F.explode("context").alias("key", "v"))

filtered = (
    exploded
    .filter(~F.col("key").eqNullSafe(F.lit("trace")))
    .select(
        "id",
        "service",
        "level",
        "key",
        "v",
        F.upper("service").alias("service_upper"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("key").isNotNull())
    .select("*", F.coalesce("key", F.lit("Unknown")).alias("key_filled"))
)

cleaned = (
    filtered_app_logs
    .filter(F.col("key").isNotNull() & (F.col("key").eqNullSafe(F.lit("trace"))))
    .select(
        "*",
        F.concat_ws(",", "key", "service").alias("key_service_key"),
        F.length("service").alias("service_length"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled_2"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "level",
        "key",
        "v",
        "service_upper",
        "key_filled",
        "key_service_key",
        "service_length",
        "key_filled_2",
    )
)

result = (
    deduped
    .groupBy("key")
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.avg(F.when(F.col("level") == "DEBUG", F.col("service_length"))).alias("debug_avg_service_length"),
        F.sum(F.when(F.col("level") == "INFO", F.col("service_length")).otherwise(0)).alias("info_service_length"),
    )
    .orderBy(F.col("num_levels").desc_nulls_last())
)
