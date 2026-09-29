from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-09-24"))) & F.col("channel").isNotNull())
    .withColumn("channel_group", F.when(F.col("channel").isin(["store", "partner"]), "Group A").otherwise("Other"))
)

matched = (
    valid
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

remaining_keys = (
    matched
    .select("channel", "promo_code")
    .intersect(
        df_promotions
        .filter(F.col("starts_on") <= "2025-03-05")
        .select("channel", "promo_code")
    )
)

kept_rows = (
    matched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

matched_promotions = (
    kept_rows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

with_window = (
    matched_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

base = (
    with_window
    .filter(F.col("promotion_promo_code_2") != "VIP")
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        "promotion_discount_pct_2",
        "promo_id_3",
        "promotion_promo_code_3",
        F.length("promotion_promo_code_2").alias("promotion_promo_code_2_length"),
    )
)

result = (
    base
    .select(
        "id",
        "channel",
        "promo_code",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promotion_discount_pct_2",
        "promo_id_3",
        "promotion_promo_code_3",
    )
    .orderBy(F.col("promotion_promo_code").desc(), "promo_code")
)
