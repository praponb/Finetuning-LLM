from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    flattened
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
    )
)
