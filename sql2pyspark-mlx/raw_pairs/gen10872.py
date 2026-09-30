from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "plan",
        "tier_value",
        F.max("tier_value").over(w).alias("plan_max_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)
