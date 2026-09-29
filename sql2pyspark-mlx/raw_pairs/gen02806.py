from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipping_fee")

result = (
    df_orders
    .filter(F.col("amount") <= 1310)
    .select("id", "status", "shipping_fee", F.rank().over(w).alias("shipping_fee_rank"))
    .orderBy("shipping_fee")
)
