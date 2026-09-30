from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date")

result = (
    df_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "order_date", "status")
    .orderBy(F.col("id").desc(), F.col("store_id").desc())
)
