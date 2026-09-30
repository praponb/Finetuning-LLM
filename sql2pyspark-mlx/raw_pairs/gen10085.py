from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("tag").orderBy(F.col("tag_length_max_15m").asc_nulls_first())
w3 = Window.orderBy(F.col("tag_length_max_15m").desc_nulls_last())
w4 = Window.partitionBy("tag").orderBy(F.col("tag_length_max_15m").desc_nulls_last())
w5 = Window.partitionBy("tag").orderBy(F.col("tag_length_max_15m_rank").desc())

lines = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("tag"))

valid = (
    lines
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 03:00:00"))) & (F.col("created_at") >= "2025-06-09"))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
    .withColumn("tag_clean", F.coalesce("tag", F.lit("N/A")))
)

filtered = (
    valid
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .withColumn("tag_length", F.length("tag"))
)

scored = (
    filtered
    .select(
        "id",
        "tag",
        "created_at_label",
        "tag_clean",
        F.max("tag_length").over(w).alias("tag_length_max_15m"),
    )
)

enriched = (
    scored
    .withColumn("tag_length_max_15m_percent_rank", F.percent_rank().over(w2))
    .withColumn("tag_length_max_15m_rank", F.rank().over(w3))
)

scored_carts = (
    enriched
    .select(
        "id",
        "tag",
        "created_at_label",
        "tag_clean",
        "tag_length_max_15m",
        "tag_length_max_15m_percent_rank",
        F.rank().over(w2).alias("tag_length_max_15m_rank_2"),
        F.rank().over(w4).alias("tag_length_max_15m_rank_3"),
        F.dense_rank().over(w5).alias("tag_length_max_15m_rank_dense_rank"),
    )
)

cleaned = (
    scored_carts
    .filter(F.col("tag_length_max_15m").isNull())
    .select(
        "id",
        "tag",
        "tag_length_max_15m",
        "tag_length_max_15m_rank_2",
        "tag_length_max_15m_rank_3",
        "tag_length_max_15m_rank_dense_rank",
        F.round((F.col("tag_length_max_15m_percent_rank") / F.col("tag_length_max_15m_rank_3")), 2).alias("tag_length_max_15m_percent_rank_per_tag_length_max_15m_rank_3"),
        F.coalesce("tag", F.lit("NONE")).alias("tag_clean_2"),
    )
)

result = (
    cleaned
    .groupBy("tag")
    .agg(
        F.round(F.sum("tag_length_max_15m_rank_dense_rank") / F.nullif(F.count(F.when(F.col("tag") == "VIP", 1)), F.lit(0)), 2).alias("tag_length_max_15m_rank_dense_rank_per_vip"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_count"),
        F.round(F.sum("tag_length_max_15m") / F.nullif(F.count(F.when(F.col("tag") == "BUNDLE", 1)), F.lit(0)), 2).alias("tag_length_max_15m_per_bundle"),
        F.round(F.avg("tag_length_max_15m_rank_dense_rank"), 2).alias("avg_tag_length_max_15m_rank_dense_rank"),
        F.count(F.when(F.col("tag") == "BUNDLE", 1)).alias("bundle_rows"),
        F.max(F.when(F.col("tag") == "WELCOME5", F.col("tag_length_max_15m_rank_dense_rank"))).alias("welcome5_max_tag_length_max_15m_rank_dense_rank"),
    )
)
