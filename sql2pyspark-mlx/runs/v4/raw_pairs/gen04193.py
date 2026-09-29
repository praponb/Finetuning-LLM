from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency")

lines = df_price_tiers.select("id", "currency", F.explode("discounts").alias("element"))

result = (
    lines
    .withColumn("currency_count_element", F.count("element").over(w_grp))
    .withColumn("currency_stddev_samp_element", F.stddev_samp("element").over(w_grp))
)
