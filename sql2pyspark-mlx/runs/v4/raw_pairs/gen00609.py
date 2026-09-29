from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())

exploded = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("val"))

result = (
    exploded
    .withColumn("plan_stddev_samp_val", F.stddev_samp("val").over(w))
    .withColumn("plan_stddev_samp_val_2", F.stddev_samp("val").over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
)
