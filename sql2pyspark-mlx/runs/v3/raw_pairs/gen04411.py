from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .select("id", "channel", "shipping_fee", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
)
