from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "host").orderBy(F.col("is_h3").desc())
w2 = Window.partitionBy("host", "service")
w3 = Window.partitionBy("level", "service").orderBy(F.col("is_h3").desc_nulls_first())
w4 = Window.partitionBy("host")
w5 = Window.partitionBy("host", "service").orderBy(F.col("is_h3").desc_nulls_last())
w6 = Window.orderBy(F.col("host_min_is_h3").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

valid = (
    flattened
    .filter(F.col("attr_key") == "version")
    .withColumn("is_h3", F.when(F.col("host") == "h3", 1).otherwise(0))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("N/A")))
)

enriched = (
    valid
    .filter(F.col("service").isin(["auth", "checkout", "search"]))
    .withColumn("is_h3_dense_rank", F.dense_rank().over(w))
    .withColumn("host_min_is_h3", F.min("is_h3").over(w2))
    .withColumn("is_h3_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("host_stddev_samp_is_h3", F.stddev_samp("is_h3").over(w4))
    .withColumn("is_h3_cume_dist", F.cume_dist().over(w5))
)

cleaned = (
    enriched
    .filter(F.col("attr_key").isNotNull() & (F.col("is_h3_dense_rank_2") < 5))
    .select(
        "*",
        F.coalesce("is_h3_dense_rank_2", F.lit(0)).alias("is_h3_dense_rank_2_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        "v",
        "attr_key_filled",
        "host_min_is_h3",
        "host_stddev_samp_is_h3",
        F.percent_rank().over(w6).alias("host_min_is_h3_percent_rank"),
        F.stddev_samp("is_h3_dense_rank_2_filled").over(w4).alias("host_stddev_samp_is_h3_dense_rank_2_filled"),
    )
)
