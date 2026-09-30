from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "promotion_promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(-5, -1)
w3 = Window.partitionBy("promo_code")
w_grp = Window.partitionBy("promo_code", "promotion_promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("promo_code").orderBy("discount_pct", "id", "promo_id").rowsBetween(-6, -1)

matched = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    matched
    .filter(F.col("channel") == "store")
    .withColumn("last4_min_promotion_discount_pct", F.min("promotion_discount_pct").over(w))
    .withColumn("last4_max_promotion_discount_pct", F.max("promotion_discount_pct").over(w))
    .withColumn("last4_avg_promotion_discount_pct", F.avg("promotion_discount_pct").over(w))
)

with_windows = (
    metrics
    .withColumn("last4_max_promotion_discount_pct_avg_5_prior", F.avg("last4_max_promotion_discount_pct").over(w_hist))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("last4_max_promotion_discount_pct_share", F.round((F.col("last4_max_promotion_discount_pct") / F.sum("last4_max_promotion_discount_pct").over(w3)), 4))
    .withColumn("first_known_promotion_discount_pct", F.first("promotion_discount_pct", ignorenulls=True).over(w_grp))
    .withColumn("promotion_discount_pct_avg_6_prior", F.avg("promotion_discount_pct").over(w_part))
)

cleaned = (
    with_windows
    .filter(F.col("ends_on") > F.lit("2023-07-27").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "last4_min_promotion_discount_pct",
        "last4_max_promotion_discount_pct_avg_5_prior",
        "discount_pct_share",
        "last4_max_promotion_discount_pct_share",
        "first_known_promotion_discount_pct",
        "promotion_discount_pct_avg_6_prior",
        F.date_add("starts_on", 79).alias("starts_on_plus_79d"),
        F.round((F.col("last4_max_promotion_discount_pct_share") / F.col("last4_avg_promotion_discount_pct")), 2).alias("last4_max_promotion_discount_pct_share_per_last4_avg_promotion_discount_pct"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("discount_pct") >= 0.34, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.percentile_approx("last4_min_promotion_discount_pct", 0.5).alias("last4_min_promotion_discount_pct_p50"),
        F.max("last4_min_promotion_discount_pct").alias("max_last4_min_promotion_discount_pct"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.avg(F.when(F.col("channel") == "mobile", F.col("last4_min_promotion_discount_pct"))).alias("mobile_avg_last4_min_promotion_discount_pct"),
    )
    .orderBy(F.col("partner_count").desc_nulls_first())
)
