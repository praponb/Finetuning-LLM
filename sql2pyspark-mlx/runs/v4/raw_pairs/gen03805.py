from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id", "promo_id").rowsBetween(-3, -1)
w2 = Window.orderBy("ends_on", "id", "promo_id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("days_starts_on_to_ends_on").desc())
w4 = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promotion_promo_code").orderBy("discount_pct_avg_3_prior", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promotion_promo_code")
w7 = Window.partitionBy("promotion_promo_code").orderBy("starts_on")
w8 = Window.partitionBy("promotion_promo_code").orderBy(F.col("discount_pct_avg_3_prior").desc(), "id", "promo_id")
w9 = Window.partitionBy("promo_code").orderBy("days_starts_on_to_ends_on_rank", "id", "promo_id")
w_part = Window.partitionBy("channel").orderBy(F.col("discount_pct_avg_3_prior").desc_nulls_last())
w_running = Window.partitionBy("promo_code")

filtered = (
    df_promotions
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-10-24")))
    .withColumn("days_starts_on_to_ends_on", F.datediff("ends_on", "starts_on"))
)

attributed = (
    filtered
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    attributed
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_3_prior"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

filtered_promotions = (
    metrics
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.when(F.col("promo_code").isin(["BUNDLE", "SPRING10"]), "Priority").otherwise("Other").alias("promo_code_group"),
        F.coalesce("promotion_promo_code", F.lit("NONE")).alias("promotion_promo_code_clean"),
    )
)

with_windows = (
    filtered_promotions
    .withColumn("days_starts_on_to_ends_on_rank", F.rank().over(w3))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w4))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w5))
    .withColumn("promotion_discount_pct_share", F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w6)), 4))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

deduped_promotions = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_promotions
    .withColumn("days_starts_on_to_ends_on_rank_bucket", F.ntile(5).over(w9))
    .withColumn("discount_pct_avg_3_prior_dense_rank", F.dense_rank().over(w_part))
    .withColumn("promotion_promo_code_stddev_samp_days_starts_on_to_ends_on_rank", F.stddev_samp("days_starts_on_to_ends_on_rank").over(w6))
    .withColumn("promo_code_min_promotion_discount_pct", F.min("promotion_discount_pct").over(w_running))
)
