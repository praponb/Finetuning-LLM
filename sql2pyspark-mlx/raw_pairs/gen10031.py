from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "effective_from"), "plan")
w2 = Window.orderBy(F.col("tier_value").asc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    exploded
    .select(
        "plan",
        "effective_from",
        F.max("tier_value").over(w).alias("effective_from_period_max_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)
