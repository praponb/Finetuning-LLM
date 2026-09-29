from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("promo_code").isin(["SPRING10", "BUNDLE"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.trim("promo_code").alias("clean_promo_code"),
    )
)

attributed = (
    valid
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

remaining_keys = (
    attributed
    .select("channel", "id")
    .intersect(
        df_promotions
        .filter(F.col("discount_pct") <= 0.15)
        .select("channel", "id")
    )
)

kept_rows = (
    attributed
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_window = (
    kept_rows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window
    .groupBy("promotion_promo_code")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("promo_id_2")).otherwise(0)).alias("vip_promo_id_2"),
        F.percentile_approx("promotion_discount_pct", 0.9).alias("promotion_discount_pct_p90"),
    )
)
