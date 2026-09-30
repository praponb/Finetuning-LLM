from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

result = (
    flattened
    .filter(F.col("val") > 73)
    .withColumn("val_dense_rank", F.dense_rank().over(w))
)
