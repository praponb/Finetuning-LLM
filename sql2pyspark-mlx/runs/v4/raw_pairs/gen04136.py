from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms_filled").desc_nulls_first())
w2 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms_filled").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

filtered = (
    flattened
    .filter(F.col("attr_key").isNotNull())
    .withColumn("latency_ms_filled", F.coalesce("latency_ms", F.lit(0)))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("OTHER")))
)

base = (
    filtered
    .filter(F.col("latency_ms") < 1509)
    .select(
        "id",
        "level",
        "attr_key",
        "latency_ms_filled",
        "attr_key_clean",
        F.when(F.col("latency_ms") >= 2260, "High").when(F.col("latency_ms") >= 833, "Medium").otherwise("Low").alias("latency_ms_tier"),
        F.upper("attr_key").alias("attr_key_upper"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

cleaned = (
    base
    .filter(F.col("level") == "ERROR")
    .withColumn("clean_attr_key", F.trim("attr_key"))
    .withColumn("resolved_attr_key_2", F.coalesce("attr_key", F.lit("OTHER")))
)

result = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_filled_dense_rank"),
        F.cume_dist().over(w2).alias("latency_ms_filled_cume_dist"),
    )
)
