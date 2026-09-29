from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("element").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

result = (
    flattened
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .select(
        "id",
        "currency",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.sum("element").over(w2).alias("plan_sum_element"),
        F.cume_dist().over(w3).alias("element_cume_dist_2"),
    )
)
