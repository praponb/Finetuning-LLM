from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("effective_from"))
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("tier_value"))
)

result = (
    exploded
    .filter(F.col("tier_value") != 66)
    .select(
        "id",
        "tier_value",
        F.avg("tier_value").over(w).alias("effective_from_year_avg_tier_value"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
    )
)
