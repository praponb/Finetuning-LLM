from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on")
w2 = Window.partitionBy("promotion_promo_code")
w3 = Window.partitionBy("promotion_promo_code_2", "promotion_promo_code")

ranked = (
    df_promotions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "channel", "promo_code", "starts_on", "discount_pct")
)

remaining_keys = (
    ranked
    .select("promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("promo_code") == "BUNDLE")
        .select("promo_code")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

with_window = (
    verified
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

cleaned = (
    with_window
    .filter(F.col("promotion_promo_code") == "BUNDLE")
    .select(
        "*",
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.coalesce("promotion_promo_code", F.lit("DIRECT")).alias("promotion_promo_code_clean"),
    )
)

with_window_promotions = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

with_window_promotions_2 = (
    with_window_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

valid = (
    with_window_promotions_2
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-05-12"))) & (F.col("discount_pct") <= 0.31))
    .select(
        "id",
        "starts_on",
        "promotion_promo_code",
        "is_bundle",
        "promotion_promo_code_clean",
        "promotion_promo_code_2",
        "promotion_discount_pct_2",
        "promotion_discount_pct_3",
        (F.col("discount_pct") + F.col("promo_id")).alias("discount_pct_plus_promo_id"),
        F.coalesce("promotion_promo_code_2", F.lit("NONE")).alias("resolved_promotion_promo_code_2"),
    )
)

result = (
    valid
    .withColumn("promotion_discount_pct_3_share", F.round((F.col("promotion_discount_pct_3") / F.sum("promotion_discount_pct_3").over(w2)), 4))
    .withColumn("promotion_discount_pct_3_share_2", F.round((F.col("promotion_discount_pct_3") / F.sum("promotion_discount_pct_3").over(w3)), 4))
)
