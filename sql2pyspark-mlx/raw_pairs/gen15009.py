from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")
w4 = Window.partitionBy("promo_code").orderBy("discount_pct")
w5 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-3, -1)
w_grp = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-08-14").cast("date")) & (F.col("starts_on") < "2025-08-29"))
    .select(
        "*",
        F.round((F.col("discount_pct") * 1.07), 2).alias("discount_pct_adjusted"),
    )
)

scored = (
    cleaned
    .withColumn("promo_code_avg_discount_pct_adjusted", F.avg("discount_pct_adjusted").over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
    .withColumn("prev_discount_pct_adjusted", F.lag("discount_pct_adjusted", 1).over(w3))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w4))
    .withColumn("discount_pct_adjusted_avg_3_prior", F.avg("discount_pct_adjusted").over(w5))
)

enriched = (
    scored
    .select(
        "id",
        "promo_code",
        "discount_pct_adjusted",
        "promo_code_avg_discount_pct_adjusted",
        "prev_discount_pct_adjusted",
        "discount_pct_cume_dist",
        "discount_pct_adjusted_avg_3_prior",
        F.avg("prev_discount_pct_adjusted").over(w2).alias("channel_avg_prev_discount_pct_adjusted"),
        F.sum("promo_code_avg_discount_pct_adjusted").over(w_grp).alias("running_promo_code_avg_discount_pct_adjusted"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct").between(0.24, 0.27))
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("discount_pct_cume_dist") <= 0.4)
    .select(
        "*",
        (F.col("running_promo_code_avg_discount_pct_adjusted") / F.nullif("discount_pct_cume_dist", F.lit(0))).alias("running_promo_code_avg_discount_pct_adjusted_per_discount_pct_cume_dist"),
    )
)

stats = (
    valid
    .groupBy("promo_code")
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.round(F.sum("discount_pct_adjusted") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("discount_pct_adjusted_per_bundle"),
    )
)

result = (
    stats
    .select(
        "promo_code",
        "promo_code_list",
        "discount_pct_adjusted_per_bundle",
        F.when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "EXTENDED").when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code_zone").desc())
)
