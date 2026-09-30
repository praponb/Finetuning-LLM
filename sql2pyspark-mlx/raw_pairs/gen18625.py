from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct")
w2 = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")
w3 = Window.orderBy("ends_on", "id", "promo_id").rowsBetween(-2, -1)
w4 = Window.partitionBy("promo_code").orderBy("rank_in_group", "id", "promo_id").rowsBetween(-5, -1)

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("ends_on").between("2024-02-01", "2025-01-01"))
    .select(
        "*",
        F.trim("promo_code").alias("clean_promo_code"),
        F.add_months("ends_on", 12).alias("ends_on_plus_months"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "promo_code", "starts_on", "ends_on")
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

matched = (
    deduped
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched = (
    matched
    .filter(F.col("starts_on") <= "2024-01-01")
    .select(
        "*",
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_2_prior"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_5_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "discount_pct",
        "discount_pct_avg_2_prior",
        "discount_pct_avg_5_prior",
        F.when(F.col("promo_code").rlike("^(VIP|BUNDLE)$"), "CORE").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").desc(), "promo_id")
)
