from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("plan").orderBy("element")
w3 = Window.partitionBy("currency").orderBy(F.col("plan_max_element").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_max_element").desc())
w5 = Window.partitionBy("currency").orderBy(F.col("element_dense_rank").desc())
w6 = Window.partitionBy("currency").orderBy(F.col("element_dense_rank_percent_rank").desc_nulls_first())
w7 = Window.partitionBy("currency")
w8 = Window.partitionBy("currency", "plan")

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
        "*",
        F.max("element").over(w).alias("plan_max_element"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
    )
)

enriched = (
    metrics
    .withColumn("plan_max_element_cume_dist", F.cume_dist().over(w3))
    .withColumn("plan_max_element_cume_dist_2", F.cume_dist().over(w4))
)

scored = (
    enriched
    .withColumn("element_dense_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("plan_avg_plan_max_element_cume_dist_2", F.avg("plan_max_element_cume_dist_2").over(w))
)

result = (
    scored
    .withColumn("element_dense_rank_percent_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("element_dense_rank_share", F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w7)), 4))
    .withColumn("currency_min_element", F.min("element").over(w8))
)
