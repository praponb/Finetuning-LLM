from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

valid = (
    flattened
    .filter(F.col("val").between(47, 54))
    .withColumn("is_basic", F.when(F.col("plan") == "basic", 1).otherwise(0))
)

valid_price_tiers = (
    valid
    .filter(F.year("effective_from") == 2023)
    .select(
        "plan",
        "val",
        "is_basic",
        F.ceil("is_basic").alias("is_basic_whole"),
        F.ceil("val").alias("val_whole"),
    )
)

result = (
    valid_price_tiers
    .select("plan", "val", "is_basic", "is_basic_whole", "val_whole")
    .orderBy(F.col("is_basic").desc_nulls_first(), "val")
)
