from pyspark.sql import functions as F

lines = (
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
    lines
    .groupBy("currency")
    .agg(
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("val_per_pro"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("val")).otherwise(0)).alias("eur_val"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
    )
    .orderBy(F.col("val_per_pro").desc_nulls_last())
)
