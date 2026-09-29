from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

valid = (
    exploded
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-02-02"))) & (F.col("effective_from") >= "2023-04-19"))
    .select(
        "effective_from",
        "currency",
        "element",
        F.floor("element").alias("element_whole"),
        F.substring("currency", 1, 3).alias("currency_prefix"),
    )
)

kept_keys = (
    valid
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
        .select("currency")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows
    .select("currency")
    .intersectAll(
        df_price_tiers
        .filter(~F.col("plan").isin(["basic", "starter", "enterprise"]))
        .select("currency")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("currency").isin(["USD", "EUR"]))
    .select("*", F.dayofmonth("effective_from").alias("effective_from_day"))
)

result = (
    cleaned
    .select(
        "effective_from",
        "currency",
        "element_whole",
        F.when(F.col("currency").rlike("^(EUR|USD)$"), "GROUP_A").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("currency_zone").desc_nulls_last(), "effective_from")
)
