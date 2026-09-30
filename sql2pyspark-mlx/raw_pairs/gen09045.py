from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w_running_2 = Window.partitionBy("host")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

filtered = (
    flattened
    .filter(F.col("level") == "WARN")
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "attr_key",
        "v",
        F.when(F.col("host") == "h1", 1).otherwise(0).alias("is_h1"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean"),
    )
)

base = (
    filtered
    .filter(F.col("status_code") != 394)
    .withColumn("host_prefix", F.substring("host", 1, 3))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

scored = (
    base
    .filter(F.col("host").isin(["h3", "h4", "h2"]))
    .select(
        "id",
        "level",
        "status_code",
        "attr_key",
        "attr_key_clean",
        "resolved_attr_key",
        F.dense_rank().over(w_running).alias("status_code_dense_rank"),
        F.min("is_h1").over(w_running_2).alias("host_min_is_h1"),
    )
)

result = (
    scored
    .select(
        "level",
        "attr_key",
        "attr_key_clean",
        "resolved_attr_key",
        "status_code_dense_rank",
        "host_min_is_h1",
    )
    .orderBy(F.col("level").desc_nulls_first())
)
