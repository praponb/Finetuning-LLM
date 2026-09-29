from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy(F.month("effective_from"), "currency").orderBy(F.col("tier_value").desc_nulls_first())
w_ordered = Window.orderBy(F.col("tier_value").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

valid = (
    flattened
    .filter(~F.col("currency").isin(["THB", "USD"]))
    .select("*", F.initcap(F.trim("currency")).alias("currency_title"))
)

metrics = (
    valid
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w_ordered))
)

filtered = (
    metrics
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-04-11")))
    .select(
        "id",
        "effective_from",
        "currency",
        "currency_title",
        "tier_value_cume_dist",
        "tier_value_dense_rank",
        "tier_value_dense_rank_2",
        F.coalesce("tier_value_dense_rank_2", F.lit(0)).alias("tier_value_dense_rank_2_filled"),
    )
)

filtered_price_tiers = (
    filtered
    .filter(F.col("currency") == "THB")
    .select(
        "id",
        "effective_from",
        "currency",
        "currency_title",
        "tier_value_cume_dist",
        "tier_value_dense_rank",
        "tier_value_dense_rank_2",
        "tier_value_dense_rank_2_filled",
        (F.col("tier_value_cume_dist") + F.col("tier_value_dense_rank_2_filled")).alias("tier_value_cume_dist_plus_tier_value_dense_rank_2_filled"),
    )
)

remaining_keys = (
    filtered_price_tiers
    .select("id", "currency")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("id", "currency")
    )
)

kept_rows = (
    filtered_price_tiers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .groupingSets(
        [["week_start", "quarter_start"], ["week_start"], []],
        "week_start",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("tier_value_dense_rank_2", [0.25, 0.5, 0.75]).alias("tier_value_dense_rank_2_quartiles"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.round(F.sum("tier_value_cume_dist") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("tier_value_cume_dist_per_thb"),
    )
)
