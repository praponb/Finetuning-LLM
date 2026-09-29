from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")
w2 = Window.orderBy(F.col("tag_length").desc_nulls_last())
w3 = Window.orderBy(F.col("tag_length_str").desc_nulls_last())
w4 = Window.partitionBy("tag").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("tag"))

valid = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-02 04:00:00").cast("timestamp")) & F.col("tag").isNotNull())
    .select(
        "*",
        F.trim("tag").alias("clean_tag"),
        F.coalesce("tag", F.lit("DIRECT")).alias("tag_filled"),
    )
)

base = (
    valid
    .filter((F.col("created_at") > F.lit("2025-03-01 17:00:00").cast("timestamp")) & F.col("tag").isNotNull())
    .withColumn("tag_length", F.length("tag"))
)

cleaned = (
    base
    .filter(F.col("created_at") > F.lit("2025-03-01 18:00:00").cast("timestamp"))
    .withColumn("tag_length_str", F.col("tag_length").cast("double"))
    .withColumn("tag_upper", F.upper("tag"))
)

metrics = (
    cleaned
    .select(
        "id",
        "tag",
        "clean_tag",
        "tag_filled",
        "tag_length_str",
        "tag_upper",
        F.stddev_samp("tag_length").over(w).alias("tag_stddev_samp_tag_length"),
        F.percent_rank().over(w2).alias("tag_length_percent_rank"),
        F.rank().over(w3).alias("tag_length_str_rank"),
        F.sum("tag_length_str").over(w4).alias("tag_length_str_sum_15m"),
    )
)

filtered = (
    metrics
    .filter(F.col("tag_length_str_sum_15m") >= 149)
    .withColumn("tag_length_str_sum_15m_rounded", F.round("tag_length_str_sum_15m", 0))
    .withColumn("resolved_tag", F.coalesce("tag", F.lit("UNKNOWN")))
)

base_carts = (
    filtered
    .filter(F.col("tag_length_str_rank") >= 7)
    .select(
        "id",
        "tag",
        "clean_tag",
        "tag_filled",
        "tag_length_str",
        "tag_upper",
        "tag_stddev_samp_tag_length",
        "tag_length_percent_rank",
        "tag_length_str_rank",
        "tag_length_str_sum_15m",
        "tag_length_str_sum_15m_rounded",
        "resolved_tag",
        F.least("tag_length_str_rank", "tag_length_str").alias("least_tag_length_str_rank_tag_length_str"),
    )
)

result = (
    base_carts
    .select(
        "tag",
        "clean_tag",
        "tag_filled",
        "tag_length_str",
        "tag_length_str_rank",
        "tag_length_str_sum_15m",
        "tag_length_str_sum_15m_rounded",
        "resolved_tag",
        "least_tag_length_str_rank_tag_length_str",
    )
)
