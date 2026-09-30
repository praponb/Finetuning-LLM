from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee")

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("id", "channel", "shipping_fee", F.dense_rank().over(w).alias("dense_rnk"))
)
