from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("currency", "plan").orderBy(F.col("element").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

result = (
    exploded
    .withColumn("currency_count_element", F.count("element").over(w))
    .withColumn("element_dense_rank", F.dense_rank().over(w2))
)
