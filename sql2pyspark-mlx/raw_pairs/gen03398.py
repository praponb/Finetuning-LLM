from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

result = (
    lines
    .rollup(
        F.when(F.col("val") >= 74, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
        "currency",
    )
    .agg(
        F.max(F.when(F.col("currency") == "USD", F.col("val"))).alias("usd_max_val"),
        F.sum(F.when(F.col("plan") == "basic", F.col("val"))).alias("basic_total_val"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("val")).otherwise(0)).alias("eur_val"),
    )
    .orderBy(F.col("eur_val").desc())
)
