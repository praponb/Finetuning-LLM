from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id", "promo_id", "promo_id_2")

valid = (
    df_promotions
    .filter(F.col("starts_on") > F.to_date(F.lit("2023-07-11")))
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.add_months("ends_on", 12).alias("ends_on_plus_months"),
        F.lower("channel").alias("channel_lower"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        "ends_on_plus_months",
        "channel_lower",
    )
)

valid_promotions = (
    latest
    .filter(F.col("discount_pct").between(0.15, 0.21))
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        "channel_lower",
        (F.datediff("ends_on_plus_months", "ends_on") / 7).alias("weeks_ends_on_to_ends_on_plus_months"),
    )
)

matched = (
    valid_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

ranked = (
    attributed
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_window = (
    ranked
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    with_window
    .groupBy(
        F.when(F.col("promotion_discount_pct_3") >= 0.27, "PREMIUM").otherwise("STANDARD").alias("promotion_discount_pct_3_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.max(F.when(F.col("promotion_promo_code") == "VIP", F.col("promotion_discount_pct_2"))).alias("vip_max_promotion_discount_pct_2"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.count(F.when(F.col("promotion_promo_code_2") == "SPRING10", 1)).alias("spring10_count"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
