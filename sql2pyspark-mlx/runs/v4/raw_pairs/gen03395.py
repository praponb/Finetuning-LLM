from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

result = (
    flattened
    .groupBy(
        "currency",
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
        F.when(F.col("val") >= 67, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
    )
    .agg(
        F.sum("val").alias("total_val"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("currency") == "USD", F.col("val")).otherwise(0)).alias("usd_val"),
        F.sum(F.when(F.col("plan") == "pro", F.col("val")).otherwise(0)).alias("pro_val"),
    )
    .filter(F.col("cnt") < 1)
)
