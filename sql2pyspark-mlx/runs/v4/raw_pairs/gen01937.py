from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("element").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.avg("element").over(w_part).alias("plan_avg_element"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
    )
)
