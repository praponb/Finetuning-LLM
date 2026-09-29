from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_prices").alias("tier_value"))
)

result = (
    exploded
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("currency_max_tier_value", F.max("tier_value").over(w2))
)
