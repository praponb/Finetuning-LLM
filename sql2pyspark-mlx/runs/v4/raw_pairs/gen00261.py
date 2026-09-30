from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct_bucket").desc())
w4 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_bucket").desc_nulls_last())
w5 = Window.partitionBy("channel", "promo_code")
w6 = Window.partitionBy("channel")
w7 = Window.partitionBy("channel").orderBy(F.col("discount_pct_bucket").desc_nulls_last())

valid = (
    df_promotions
    .filter(F.col("promo_code") == "WELCOME5")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("channel").isin(["partner", "web"]), "Core").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.ntile(10).over(w_hist).alias("discount_pct_bucket"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
    )
)

matched_keys = (
    scored
    .select("promo_code", "id")
    .subtract(
        df_promotions
        .filter(F.col("starts_on") < "2024-12-28")
        .select("promo_code", "id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.rank().over(w3).alias("discount_pct_bucket_rank"),
        F.rank().over(w4).alias("discount_pct_bucket_rank_2"),
        F.round((F.col("discount_pct_bucket") / F.sum("discount_pct_bucket").over(w5)), 4).alias("discount_pct_bucket_share"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w6)), 4).alias("discount_pct_share"),
    )
)

valid_promotions = (
    with_windows
    .filter(F.col("discount_pct_share") <= 0.29)
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "channel_group",
        "channel_filled",
        "discount_pct_bucket",
        "first_known_channel",
        "discount_pct_bucket_rank",
        "discount_pct_bucket_rank_2",
        "discount_pct_bucket_share",
        "discount_pct_share",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
    )
)

metrics = (
    valid_promotions
    .select(
        "*",
        F.dense_rank().over(w7).alias("discount_pct_bucket_dense_rank"),
        F.round((F.col("discount_pct_bucket") / F.sum("discount_pct_bucket").over(w6)), 4).alias("discount_pct_bucket_share_2"),
    )
)

result = (
    metrics
    .groupBy("channel")
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.avg("discount_pct_share").alias("avg_discount_pct_share"),
        F.sum("discount_pct").alias("total_discount_pct"),
    )
    .filter(F.col("num_channels") >= 1)
)
