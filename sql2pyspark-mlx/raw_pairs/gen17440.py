from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    lines
    .filter(F.col("plan").isin(["starter", "enterprise", "pro"]))
    .select(
        "id",
        "plan",
        "tier_value",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
    )
)
