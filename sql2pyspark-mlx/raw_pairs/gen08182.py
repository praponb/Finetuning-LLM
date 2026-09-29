from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy(F.month("starts_on"), "promo_code").orderBy("discount_pct")
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code").orderBy("discount_pct_rank", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("promo_code")
w6 = Window.partitionBy("promo_code").orderBy("discount_pct_rank", "id").rowsBetween(-6, -1)
w7 = Window.orderBy("discount_pct_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("discount_pct_rank_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("first_known_discount_pct_bucket_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share").desc(), "id")
w11 = Window.partitionBy(F.month("starts_on"))
w12 = Window.orderBy("first_known_discount_pct_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        F.min("discount_pct").over(w).alias("channel_min_discount_pct"),
        F.rank().over(w2).alias("discount_pct_rank"),
        F.ntile(4).over(w3).alias("discount_pct_bucket"),
    )
)

scored = (
    enriched
    .filter(F.col("starts_on") > "2024-08-27")
    .select(
        "id",
        "starts_on",
        "discount_pct_rank",
        "discount_pct_bucket",
        F.avg("discount_pct_rank").over(w4).alias("discount_pct_rank_avg_2"),
        F.round((F.col("discount_pct_bucket") / F.sum("discount_pct_bucket").over(w5)), 4).alias("discount_pct_bucket_share"),
        F.avg("discount_pct_rank").over(w6).alias("discount_pct_rank_avg_6_prior"),
    )
)

cleaned = (
    scored
    .filter(F.col("starts_on") > F.to_date(F.lit("2023-05-24")))
    .select(
        "*",
        (F.col("discount_pct_bucket_share") * F.col("discount_pct_rank")).alias("discount_pct_bucket_share_times_discount_pct_rank"),
    )
)

cleaned_promotions = (
    cleaned
    .filter(F.col("starts_on") >= F.lit("2023-09-13").cast("date"))
    .select(
        "id",
        "starts_on",
        "discount_pct_rank",
        "discount_pct_rank_avg_2",
        "discount_pct_bucket_share",
        "discount_pct_rank_avg_6_prior",
        (F.col("discount_pct_bucket_share_times_discount_pct_rank") / F.nullif("discount_pct_bucket_share", F.lit(0))).alias("discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share"),
    )
)

with_windows = (
    cleaned_promotions
    .withColumn("first_known_discount_pct_bucket_share", F.first("discount_pct_bucket_share", ignorenulls=True).over(w7))
    .withColumn("first_known_discount_pct_rank", F.first("discount_pct_rank", ignorenulls=True).over(w8))
    .withColumn("last_known_discount_pct_rank_avg_2", F.last("discount_pct_rank_avg_2", ignorenulls=True).over(w8))
)

enriched_promotions = (
    with_windows
    .select(
        "*",
        F.sum("first_known_discount_pct_rank").over(w9).alias("running_first_known_discount_pct_rank"),
        F.ntile(4).over(w10).alias("discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share_bucket"),
        F.count("discount_pct_rank_avg_6_prior").over(w11).alias("starts_on_month_count_discount_pct_rank_avg_6_prior"),
        F.last("discount_pct_bucket_share", ignorenulls=True).over(w12).alias("last_known_discount_pct_bucket_share"),
    )
)

valid = (
    enriched_promotions
    .filter(F.col("starts_on") >= F.lit("2023-09-09").cast("date"))
    .select(
        "id",
        "starts_on",
        "discount_pct_rank",
        "discount_pct_rank_avg_2",
        "discount_pct_rank_avg_6_prior",
        "discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share",
        "first_known_discount_pct_bucket_share",
        "first_known_discount_pct_rank",
        "last_known_discount_pct_rank_avg_2",
        "running_first_known_discount_pct_rank",
        "discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share_bucket",
        F.round((F.col("discount_pct_rank") / F.col("discount_pct_rank_avg_2")), 2).alias("discount_pct_rank_per_discount_pct_rank_avg_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "starts_on",
        "discount_pct_rank",
        "discount_pct_rank_avg_2",
        "discount_pct_rank_avg_6_prior",
        "first_known_discount_pct_bucket_share",
        "last_known_discount_pct_rank_avg_2",
        "running_first_known_discount_pct_rank",
        "discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share_bucket",
    )
    .orderBy("id")
    .limit(5)
)
