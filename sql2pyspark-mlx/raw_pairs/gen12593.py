from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date")

result = (
    df_orders
    .select("id", "store_id", "order_date", F.dense_rank().over(w).alias("dense_rnk"))
)
