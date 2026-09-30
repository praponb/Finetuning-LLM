from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency", "plan").orderBy("element")
w3 = Window.partitionBy("currency").orderBy(F.col("element").desc())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

result = (
    lines
    .select(
        "*",
        F.max("element").over(w).alias("plan_max_element"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
    )
)
