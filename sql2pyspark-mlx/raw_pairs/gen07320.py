from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    exploded
    .select(
        "effective_from",
        "currency",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)
