from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("element_dense_rank").desc_nulls_last())
w4 = Window.partitionBy("plan")
w5 = Window.orderBy(F.col("element").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

metrics = (
    flattened
    .select(
        "plan",
        "effective_from",
        "element",
        F.rank().over(w_part).alias("element_rank"),
    )
)

scored = metrics.withColumn("element_dense_rank", F.dense_rank().over(w2))

with_windows = (
    scored
    .withColumn("element_dense_rank_rank", F.rank().over(w3))
    .withColumn("plan_sum_element", F.sum("element").over(w4))
    .withColumn("element_rank_2", F.rank().over(w5))
)

metrics_price_tiers = (
    with_windows
    .select(
        "plan",
        "element",
        "element_dense_rank",
        F.dense_rank().over(w5).alias("element_dense_rank_2"),
    )
)

result = metrics_price_tiers.select("plan")
