from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

result = (
    flattened
    .select(
        "effective_from",
        "currency",
        "val",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("currency_zone"),
    )
    .orderBy(F.col("currency_zone").asc_nulls_first())
)
