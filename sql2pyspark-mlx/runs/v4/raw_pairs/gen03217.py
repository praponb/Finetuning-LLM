from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

cleaned = (
    flattened
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .withColumn("plan_currency_key", F.concat_ws("/", "plan", "currency"))
)

base = (
    cleaned
    .filter(F.col("currency") != "EUR")
    .select(
        "*",
        F.when(F.col("val") >= 68, "High").when(F.col("val") >= 38, "Medium").otherwise("Low").alias("val_level"),
    )
)

result = (
    base
    .select(
        "plan",
        "currency",
        "val",
        "val_level",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "GROUP_A").when(F.col("currency").rlike("^(USD)$"), "CORE").otherwise("EXTENDED").alias("currency_zone"),
    )
    .orderBy(F.col("currency_zone").desc_nulls_last())
)
