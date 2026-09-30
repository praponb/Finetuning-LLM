from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.orderBy("element")
w3 = Window.orderBy(F.col("element").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

result = (
    exploded
    .withColumn("element_cume_dist", F.cume_dist().over(w))
    .withColumn("element_cume_dist_2", F.cume_dist().over(w2))
    .withColumn("element_dense_rank", F.dense_rank().over(w3))
)
