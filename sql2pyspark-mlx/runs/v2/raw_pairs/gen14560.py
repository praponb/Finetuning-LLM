from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipping_fee")

result = (
    df_orders
    .select("id", "status", "shipping_fee", F.dense_rank().over(w).alias("dense_rnk"))
)
