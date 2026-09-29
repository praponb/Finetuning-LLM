from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost")

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-02-06")
    .select("id", "order_id", "cost", F.dense_rank().over(w).alias("dense_rnk"))
)
