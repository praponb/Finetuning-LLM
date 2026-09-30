from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id")
w2 = Window.partitionBy("promotion_promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promotion_promo_code", "promo_code")
w4 = Window.orderBy("starts_on", "id", "promo_id")
w5 = Window.partitionBy("promo_code")
w6 = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id")
w_running = Window.orderBy("next_promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    matched
    .withColumn("promotion_discount_pct_bucket", F.ntile(10).over(w))
    .withColumn("discount_pct_rank", F.rank().over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("next_promotion_discount_pct", F.lead("promotion_discount_pct", 2).over(w4))
)

scored_promotions = (
    scored
    .filter(F.col("next_promotion_discount_pct") < 0.25)
    .select(
        "id",
        "promo_id",
        "discount_pct_rank",
        "next_promotion_discount_pct",
        F.round((F.col("discount_pct_rank") / F.sum("discount_pct_rank").over(w5)), 4).alias("discount_pct_rank_share"),
        F.lead("discount_pct_rank", 1).over(w6).alias("next_discount_pct_rank"),
        F.last("promotion_promo_code", ignorenulls=True).over(w_running).alias("last_known_promotion_promo_code"),
    )
)

with_windows = (
    scored_promotions
    .withColumn("last_known_discount_pct_rank", F.last("discount_pct_rank", ignorenulls=True).over(w8))
    .withColumn("first_known_promo_id", F.first("promo_id", ignorenulls=True).over(w_running))
)

cleaned = (
    with_windows
    .filter(F.col("promo_id").isNotNull())
    .select(
        "*",
        F.coalesce("next_promotion_discount_pct", F.lit(0)).alias("next_promotion_discount_pct_filled"),
        F.round((F.col("next_discount_pct_rank") / F.col("last_known_discount_pct_rank")), 2).alias("next_discount_pct_rank_per_last_known_discount_pct_rank"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("next_discount_pct_rank") >= 7, "LARGE").otherwise("SMALL").alias("next_discount_pct_rank_tier"),
    )
    .agg(
        F.percentile_approx("next_promotion_discount_pct_filled", [0.25, 0.5, 0.75]).alias("next_promotion_discount_pct_filled_quartiles"),
        F.percentile_approx("discount_pct_rank", 0.5).alias("discount_pct_rank_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.max("promo_id").alias("max_promo_id"),
        F.max("last_known_discount_pct_rank").alias("max_last_known_discount_pct_rank"),
    )
)
