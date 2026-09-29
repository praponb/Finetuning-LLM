from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc())
w5 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct_bucket").desc_nulls_last())
w6 = Window.partitionBy("channel")
w7 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w8 = Window.partitionBy("channel").orderBy("discount_pct_bucket", "id")

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-10-13"))) & F.col("channel").isNotNull() & (F.col("discount_pct") <= 0.28))
    .select(
        "*",
        F.concat_ws(",", "channel", "promo_code").alias("channel_promo_code_key"),
    )
)

metrics = (
    filtered
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("discount_pct_bucket", F.ntile(3).over(w2))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w3))
)

enriched = (
    metrics
    .select(
        "*",
        F.rank().over(w4).alias("discount_pct_rank"),
        F.cume_dist().over(w5).alias("discount_pct_bucket_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round((F.col("running_discount_pct") / F.sum("running_discount_pct").over(w6)), 4).alias("running_discount_pct_share"),
        F.lead("discount_pct_bucket", 1, 0).over(w7).alias("next_discount_pct_bucket"),
        F.ntile(10).over(w8).alias("discount_pct_bucket_bucket"),
    )
)

matched = (
    with_windows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & F.col("s.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .groupBy(F.date_trunc("week", "ends_on").alias("week_start"))
    .agg(
        F.round(F.sum("running_discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("running_discount_pct_per_spring10"),
        F.sum(F.when(F.col("promotion_promo_code") == "SPRING10", F.col("running_discount_pct")).otherwise(0)).alias("spring10_running_discount_pct"),
        F.percentile_approx("discount_pct_bucket_cume_dist", [0.25, 0.5, 0.75]).alias("discount_pct_bucket_cume_dist_quartiles"),
        F.round(F.sum("promotion_discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("promotion_discount_pct_per_partner"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("discount_pct_bucket_bucket", 0.5).alias("discount_pct_bucket_bucket_p50"),
    )
)
