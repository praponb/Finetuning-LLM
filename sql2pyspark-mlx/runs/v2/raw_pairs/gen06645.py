from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount")

result = (
    df_orders
    .filter(F.col("shipping_fee") < 27)
    .select("id", "status", "amount", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy("id")
)
