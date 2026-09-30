from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level", "key")
w3 = Window.partitionBy("level", "key").orderBy("is_error")
w4 = Window.partitionBy("key").orderBy("is_error")

exploded = (
    df_app_logs
    .select("id", "level", "latency_ms", F.explode_outer("context").alias("key", "v"))
)

filtered = (
    exploded
    .filter(F.col("level") != "DEBUG")
    .select(
        "id",
        "level",
        "key",
        "v",
        F.when(F.col("level") == "ERROR", 1).otherwise(0).alias("is_error"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("is_error").between(63, 64))
    .withColumn("level_title", F.initcap(F.trim("level")))
    .withColumn("key_filled_2", F.coalesce("key", F.lit("UNKNOWN")))
)

base = (
    valid
    .filter(~F.col("key").isin(["version", "region"]))
    .select(
        "*",
        F.when(F.col("is_error") >= 71, "High").when(F.col("is_error") >= 37, "Medium").otherwise("Low").alias("is_error_band"),
        F.trim("level").alias("clean_level"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

enriched = (
    base
    .filter(F.col("level") == "WARN")
    .withColumn("level_avg_is_error", F.avg("is_error").over(w))
    .withColumn("level_min_is_error", F.min("is_error").over(w2))
    .withColumn("is_error_percent_rank", F.percent_rank().over(w3))
    .withColumn("is_error_percent_rank_2", F.percent_rank().over(w4))
)

result = (
    enriched
    .groupBy("level")
    .pivot("key", ["region", "version"])
    .agg(F.sum("level_avg_is_error").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
