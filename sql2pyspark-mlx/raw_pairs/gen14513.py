from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.year("ends_on"), "promo_code").orderBy("discount_pct", "id")
w_part = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_bucket_2_num").desc())
w4 = Window.partitionBy("promo_code")

base = (
    df_promotions
    .filter(~F.col("promo_code").isin(["WELCOME5", "BUNDLE", "SPRING10"]) & (F.col("starts_on") > F.lit("2023-09-28").cast("date")))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("channel").isin(["store", "web"]), "Priority").otherwise("Other").alias("channel_group"),
        F.length("channel").alias("channel_length"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

with_windows = (
    base
    .filter(F.col("promo_code") == "VIP")
    .select(
        "id",
        "promo_code",
        "ends_on",
        "channel_group",
        "channel_length",
        "channel_clean",
        F.ntile(10).over(w_hist).alias("discount_pct_bucket"),
        F.ntile(3).over(w_part).alias("discount_pct_bucket_2"),
    )
)

base_promotions = (
    with_windows
    .filter((F.col("ends_on") > F.lit("2023-09-09").cast("date")) & (F.col("ends_on") > "2025-05-20"))
    .select(
        "*",
        (F.col("channel_length") + F.col("discount_pct_bucket_2")).alias("channel_length_plus_discount_pct_bucket_2"),
    )
)

filtered = (
    base_promotions
    .filter(F.col("discount_pct_bucket").between(6, 7))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "channel_group",
        "channel_clean",
        "discount_pct_bucket",
        "discount_pct_bucket_2",
        F.col("discount_pct_bucket_2").cast("double").alias("discount_pct_bucket_2_num"),
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
    )
)

enriched = (
    filtered
    .withColumn("discount_pct_bucket_2_num_dense_rank", F.dense_rank().over(w3))
    .withColumn("promo_code_avg_discount_pct_bucket", F.avg("discount_pct_bucket").over(w4))
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("discount_pct_bucket_2_num_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("discount_pct_bucket_2_num_dense_rank_tier"),
    )
    .agg(
        F.max("discount_pct_bucket_2_num_dense_rank").alias("max_discount_pct_bucket_2_num_dense_rank"),
        F.count_if(F.col("promo_code") == "WELCOME5").alias("welcome5_events"),
        F.percentile_approx("discount_pct_bucket_2_num", 0.95).alias("discount_pct_bucket_2_num_p95"),
    )
)

result = (
    grouped
    .select(
        "discount_pct_bucket_2_num_dense_rank_tier",
        "discount_pct_bucket_2_num_p95",
    )
    .orderBy(
        F.col("discount_pct_bucket_2_num_dense_rank_tier").desc(),
        "discount_pct_bucket_2_num_p95",
    )
)
