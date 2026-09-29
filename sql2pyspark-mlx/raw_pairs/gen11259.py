from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

filtered = (
    lines
    .filter(F.col("currency") == "EUR")
    .select(
        "*",
        F.round((F.col("val") * 100), 2).alias("val_scaled"),
        F.substring("currency", 1, 4).alias("currency_prefix"),
    )
)

matched_keys = (
    filtered
    .select("id", "currency")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") == "EUR")
        .select("id", "currency")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("plan", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") > "2023-08-17")
        .select("plan", "id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.when(F.col("val") >= 64, "HIGH").otherwise("LOW").alias("val_tier"))
    .agg(
        F.percentile_approx("val_scaled", [0.25, 0.5, 0.75]).alias("val_scaled_quartiles"),
        F.percentile_approx("val_scaled", 0.9).alias("val_scaled_p90"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("val_per_enterprise"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
    )
    .filter(F.col("val_scaled_p90") > 0)
    .orderBy(F.col("val_per_enterprise").desc())
)
