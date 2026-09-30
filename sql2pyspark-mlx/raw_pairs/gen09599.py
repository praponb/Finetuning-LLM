from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("effective_from"), "currency").orderBy(F.col("tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .filter(F.col("tier_value") < 80)
    .select(
        "id",
        "effective_from",
        "currency",
        F.rank().over(w).alias("tier_value_rank"),
    )
)
