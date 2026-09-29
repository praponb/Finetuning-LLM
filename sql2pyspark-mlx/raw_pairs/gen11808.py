from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("promo_id", "id", "promo_id", "promo_id_2", "promo_id_3").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("promo_code").orderBy("promo_id_3", "id", "promo_id", "promo_id_2", "promo_id_3").rowsBetween(-3, -1)

valid = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-05-23").cast("date")) & (F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.add_months("ends_on", 1).alias("ends_on_plus_months"),
    )
)

matched = (
    valid
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched_promotions = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

filtered = (
    matched_promotions
    .filter((F.col("ends_on") > F.lit("2023-05-26").cast("date")) & F.month("ends_on_plus_months").isin([1, 2, 9]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "ends_on_plus_months",
        "promo_id",
        "promotion_promo_code",
        "promo_id_2",
        "promotion_promo_code_2",
        "promotion_discount_pct_2",
        "promo_id_3",
        "promotion_promo_code_3",
        F.ceil("promotion_discount_pct").alias("promotion_discount_pct_whole"),
        F.lower("promo_code").alias("promo_code_lower"),
    )
)

with_windows = (
    filtered
    .filter(F.col("ends_on") >= "2025-03-07")
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promo_id_2",
        "promo_id_3",
        "promotion_promo_code_3",
        "promo_code_lower",
        F.last("promotion_promo_code_3", ignorenulls=True).over(w_grp).alias("last_known_promotion_promo_code_3"),
        F.round((F.col("promotion_discount_pct_2") / F.sum("promotion_discount_pct_2").over(w2)), 4).alias("promotion_discount_pct_2_share"),
        F.avg("promotion_discount_pct_2").over(w3).alias("promotion_discount_pct_2_avg_3_prior"),
    )
)

pivoted = (
    with_windows
    .groupBy("promo_code")
    .pivot("promotion_promo_code_3", ["WELCOME5", "SPRING10"])
    .agg(F.sum("promotion_discount_pct_2_avg_3_prior"))
)

result = (
    pivoted
    .select(
        "promo_code",
        "WELCOME5",
        F.when(F.col("promo_code").rlike("^(BUNDLE|VIP)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("promo_code_zone"),
    )
)
