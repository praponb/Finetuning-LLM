from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency")

flattened = df_price_tiers.select("id", "currency", F.explode_outer("tier_limits").alias("val"))

cleaned = (
    flattened
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .select("*", F.lower("currency").alias("currency_lower"))
)

scored = cleaned.select("*", F.avg("val").over(w_recent).alias("currency_avg_val"))

valid = (
    scored
    .filter(F.col("val") == 37)
    .select(
        "id",
        "currency_avg_val",
        F.least("currency_avg_val", "val").alias("least_currency_avg_val_val"),
    )
)

result = (
    valid
    .rollup(
        F.when(F.col("currency_avg_val") >= 82, "HIGH").otherwise("LOW").alias("currency_avg_val_tier"),
    )
    .agg(
        F.min("least_currency_avg_val_val").alias("min_least_currency_avg_val_val"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("min_least_currency_avg_val_val") > 0)
    .orderBy("min_least_currency_avg_val_val")
)
