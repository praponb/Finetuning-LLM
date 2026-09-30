from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w_running = Window.partitionBy("plan")
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.orderBy("tier_value")

lines = (
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
    lines
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("plan_min_tier_value", F.min("tier_value").over(w_running))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w4))
    .withColumn("tier_value_rank_2", F.rank().over(w5))
)
