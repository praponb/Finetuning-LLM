from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promotion_promo_code").orderBy(F.col("running_avg_promo_id").desc_nulls_last())
w3 = Window.partitionBy("promo_code").orderBy("running_count_promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(-3, -1)

valid = (
    df_promotions
    .filter(F.col("discount_pct").between(0.14, 0.15) & (F.col("ends_on") > F.lit("2023-12-08").cast("date")))
    .select(
        "*",
        F.concat_ws("-", "promo_code", "channel").alias("promo_code_channel_key"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

with_window = (
    valid
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    with_window
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_code_channel_key",
        "resolved_channel",
        "promo_id",
        "promotion_promo_code",
        F.count("promo_id").over(w).alias("running_count_promo_id"),
        F.sum("promo_id").over(w).alias("running_sum_promo_id"),
        F.max("promo_id").over(w).alias("running_max_promo_id"),
        F.avg("promo_id").over(w).alias("running_avg_promo_id"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .withColumn("running_max_running_avg_promo_id", F.max("running_avg_promo_id").over(w3))
    .withColumn("running_avg_running_avg_promo_id", F.avg("running_avg_promo_id").over(w3))
    .withColumn("running_sum_running_avg_promo_id", F.sum("running_avg_promo_id").over(w3))
    .withColumn("running_count_running_avg_promo_id", F.count("running_avg_promo_id").over(w3))
    .withColumn("running_count_promo_id_avg_3_prior", F.avg("running_count_promo_id").over(w4))
)

grouped = (
    scored
    .groupBy(
        F.when(F.col("running_avg_promo_id") >= 10.4, "HIGH").otherwise("LOW").alias("running_avg_promo_id_tier"),
        "promo_id",
        F.when(F.col("discount_pct") >= 0.27, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.count(F.when(F.col("promotion_promo_code") == "VIP", 1)).alias("vip_count"),
        F.count_if(F.col("promotion_promo_code") == "BUNDLE").alias("bundle_events"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
    )
    .filter(F.col("num_promotion_promo_codes") >= 1)
)

result = (
    grouped
    .select(
        "running_avg_promo_id_tier",
        "promo_id",
        "discount_pct_tier",
        "bundle_events",
        "promo_code_list",
        "num_promotion_promo_codes",
    )
    .orderBy("discount_pct_tier")
)
