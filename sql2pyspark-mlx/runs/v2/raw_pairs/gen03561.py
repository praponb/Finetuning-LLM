from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .filter(F.col("customer_id") == 27)
    .select("id", "store_id", "order_date", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .orderBy(F.col("id").desc())
)
