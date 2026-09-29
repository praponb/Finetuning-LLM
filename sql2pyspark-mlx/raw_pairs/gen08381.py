from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

kept_keys = (
    lines
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(~F.col("plan").isin(["enterprise", "pro"]))
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-05-13")))
    .select(
        "effective_from",
        "currency",
        "element",
        F.concat("currency", F.lit(" - "), "plan").alias("currency_plan"),
    )
)

filtered = (
    valid
    .filter(F.col("element") >= 57)
    .select("*", F.date_add("effective_from", 72).alias("effective_from_plus_72d"))
)

kept_keys_2 = (
    filtered
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") > "2025-03-01")
        .select("currency")
    )
)

kept_rows_price_tiers = (
    filtered
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows_price_tiers
    .filter((F.col("effective_from") >= F.lit("2024-04-18").cast("date")) & (F.col("effective_from") >= "2023-11-13"))
    .select("*", F.add_months("effective_from", 1).alias("effective_from_plus_months"))
)

valid_price_tiers = (
    cleaned
    .filter(F.col("currency").isin(["EUR", "USD"]))
    .withColumn("effective_from_plus_months_minus_39d", F.date_sub("effective_from_plus_months", 39))
)

result = (
    valid_price_tiers
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"), "currency")
    .agg(
        F.avg("element").alias("avg_element"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("currency") == "USD", F.col("element")).otherwise(0)).alias("usd_element"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("avg_element") > 0)
)
