from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w2 = Window.orderBy("element")
w3 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    exploded
    .select(
        "id",
        "plan",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
    )
)
