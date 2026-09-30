from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("element")
w_part = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("element"))
)

result = (
    lines
    .filter(F.col("plan") == "pro")
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.min("element").over(w_part).alias("plan_min_element"),
        F.max("element").over(w_part).alias("plan_max_element"),
    )
)
