from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("effective_from"))
w2 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

metrics = (
    exploded
    .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
    .withColumn("effective_from_month_min_tier_value", F.min("tier_value").over(w))
)

enriched = (
    metrics
    .select(
        "id",
        "effective_from",
        F.avg("effective_from_month_min_tier_value").over(w2).alias("currency_avg_effective_from_month_min_tier_value"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency") == "THB")
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("year", "effective_from").alias("year_start"))
    .agg(
        F.percentile_approx("currency_avg_effective_from_month_min_tier_value", 0.5).alias("currency_avg_effective_from_month_min_tier_value_p50"),
        F.avg("currency_avg_effective_from_month_min_tier_value").alias("avg_currency_avg_effective_from_month_min_tier_value"),
        F.max("currency_avg_effective_from_month_min_tier_value").alias("max_currency_avg_effective_from_month_min_tier_value"),
        F.sum("currency_avg_effective_from_month_min_tier_value").alias("total_currency_avg_effective_from_month_min_tier_value"),
        F.percentile_approx("currency_avg_effective_from_month_min_tier_value", [0.25, 0.5, 0.75]).alias("currency_avg_effective_from_month_min_tier_value_quartiles"),
        F.percentile_approx("currency_avg_effective_from_month_min_tier_value", [0.25, 0.5, 0.75]).alias("currency_avg_effective_from_month_min_tier_value_quartiles_2"),
    )
)
