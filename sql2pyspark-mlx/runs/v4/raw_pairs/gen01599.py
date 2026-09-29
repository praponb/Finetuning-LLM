from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

result = (
    exploded
    .withColumn("val_tier", F.when(F.col("val") >= 63, "LARGE").otherwise("SMALL"))
    .groupingSets([["currency", "val_tier"], ["currency"]], "currency", "val_tier")
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.max(F.when(F.col("currency") == "THB", F.col("val"))).alias("thb_max_val"),
        F.max("val").alias("max_val"),
        F.sum(F.when(F.col("currency") == "THB", F.col("val")).otherwise(0)).alias("thb_val"),
    )
    .orderBy(F.col("val_tier").desc_nulls_last())
)
