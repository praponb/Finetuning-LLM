from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("is_warn").asc_nulls_last())
w2 = Window.partitionBy("k").orderBy(F.col("is_warn").asc_nulls_last())
w3 = Window.partitionBy("level")

lines = df_app_logs.select("id", "level", F.explode_outer("context").alias("k", "value"))

filtered = (
    lines
    .filter(F.col("k").isNotNull() & F.col("k").isNotNull())
    .select(
        "*",
        F.concat("level", F.lit(", "), "k").alias("level_k"),
        F.initcap(F.trim("level")).alias("level_title"),
    )
)

valid = (
    filtered
    .filter(F.col("k").isin(["version", "trace"]))
    .select(
        "level",
        "k",
        "value",
        "level_k",
        "level_title",
        F.when(F.col("level") == "WARN", 1).otherwise(0).alias("is_warn"),
        F.concat_ws("-", "k", "level").alias("k_level_key"),
    )
)

scored = (
    valid
    .filter(~F.col("level").isin(["DEBUG", "INFO"]))
    .withColumn("is_warn_dense_rank", F.dense_rank().over(w))
    .withColumn("is_warn_rank", F.rank().over(w2))
    .withColumn("level_min_is_warn", F.min("is_warn").over(w3))
)

filtered_app_logs = (
    scored
    .filter(F.col("is_warn_dense_rank") == 4)
    .select(
        "level",
        "value",
        "level_k",
        "is_warn",
        "k_level_key",
        "level_min_is_warn",
        F.coalesce("k", F.lit("Unknown")).alias("k_filled"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

result = (
    filtered_app_logs
    .select("level", "value", "level_k", "k_level_key", "k_filled")
    .orderBy(F.col("level").desc())
)
