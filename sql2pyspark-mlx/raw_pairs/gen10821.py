from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("is_store").desc_nulls_last())
w5 = Window.orderBy("first_known_is_store", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy("is_store_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("ends_on") <= "2025-10-07")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.lower("promo_code").alias("promo_code_lower"),
        F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

metrics = (
    base
    .withColumn("first_known_is_store", F.first("is_store", ignorenulls=True).over(w))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w2))
    .withColumn("next_discount_pct", F.lead("discount_pct", 2).over(w3))
)

valid = (
    metrics
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.round("is_store", 2).alias("is_store_rounded"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

valid_promotions = (
    valid
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-05-07"))) & (F.col("is_store_rounded") <= 39))
    .select(
        "*",
        (F.col("is_store_rounded") / F.nullif("first_known_is_store", F.lit(0))).alias("is_store_rounded_per_first_known_is_store"),
    )
)

with_windows = (
    valid_promotions
    .filter(F.col("prev_discount_pct") >= 0.31)
    .withColumn("is_store_dense_rank", F.dense_rank().over(w4))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w5))
    .withColumn("cumulative_next_discount_pct", F.sum("next_discount_pct").over(w6))
)

result = (
    with_windows
    .groupBy("channel")
    .agg(
        F.avg(F.when(F.col("promo_code") == "VIP", F.col("discount_pct"))).alias("vip_avg_discount_pct"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount_pct")).otherwise(0)).alias("web_discount_pct"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.percentile_approx("is_store_rounded", 0.9).alias("is_store_rounded_p90"),
        F.percentile_approx("prev_discount_pct", 0.9).alias("prev_discount_pct_p90"),
        F.avg(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct"))).alias("spring10_avg_discount_pct"),
    )
    .orderBy(F.col("channel").desc())
)
