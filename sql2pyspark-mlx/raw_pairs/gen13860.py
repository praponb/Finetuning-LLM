from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promotion_promo_code_3").orderBy("promotion_discount_pct_3", "id", "promo_id", "promo_id_2", "promo_id_3").rowsBetween(-2, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-05-30").cast("date"))
    .select("*", F.round("discount_pct", 2).alias("discount_pct_rounded"))
)

attributed = (
    valid
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched = (
    with_window
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

metrics = (
    matched
    .filter(~F.col("promotion_promo_code_3").isin(["BUNDLE", "WELCOME5"]))
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "promo_id",
        "promo_id_2",
        "promo_id_3",
        "promotion_promo_code_3",
        "promotion_discount_pct_3",
        F.max("promotion_discount_pct").over(w_grp).alias("last3_max_promotion_discount_pct"),
        F.avg("promotion_discount_pct").over(w_grp).alias("last3_avg_promotion_discount_pct"),
        F.count("promotion_discount_pct").over(w_grp).alias("last3_count_promotion_discount_pct"),
        F.sum("promotion_discount_pct").over(w_grp).alias("last3_sum_promotion_discount_pct"),
    )
)

result = (
    metrics
    .select(
        "id",
        "discount_pct",
        "promo_id",
        "promo_id_2",
        "promo_id_3",
        "promotion_promo_code_3",
        "promotion_discount_pct_3",
        "last3_max_promotion_discount_pct",
        "last3_avg_promotion_discount_pct",
        F.when(F.col("promotion_promo_code_3").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("GROUP_B").alias("promotion_promo_code_3_zone"),
    )
    .orderBy(F.col("promotion_discount_pct_3").desc_nulls_first(), "promo_id_3")
)
