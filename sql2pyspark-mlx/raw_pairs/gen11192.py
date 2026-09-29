from pyspark.sql import functions as F

with_window = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed = (
    with_window
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

cleaned = (
    attributed
    .filter(F.month("starts_on").isin([4, 5, 9]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        F.col("promotion_discount_pct_2").cast("int").alias("promotion_discount_pct_2_int"),
        F.round("promotion_discount_pct_2", 0).alias("promotion_discount_pct_2_rounded"),
    )
)

valid = (
    cleaned
    .filter(F.col("promotion_promo_code_2").isin(["VIP", "BUNDLE"]))
    .select(
        "id",
        "channel",
        "starts_on",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_discount_pct_2_int",
        "promotion_discount_pct_2_rounded",
        F.round((F.col("promotion_discount_pct") / F.col("promo_id_2")), 2).alias("promotion_discount_pct_per_promo_id_2"),
    )
)

result = (
    valid
    .withColumn("activity_month", F.date_trunc("MONTH", "starts_on"))
    .withColumn("promotion_discount_pct_per_promo_id_2_tier", F.when(F.col("promotion_discount_pct_per_promo_id_2") >= 84, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["activity_month", "promotion_discount_pct_per_promo_id_2_tier"], ["activity_month"], []],
        "activity_month",
        "promotion_discount_pct_per_promo_id_2_tier",
    )
    .agg(
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("promotion_discount_pct", [0.25, 0.5, 0.75]).alias("promotion_discount_pct_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("web_events") < 1000)
    .orderBy(F.col("mobile_count").asc_nulls_last())
)
