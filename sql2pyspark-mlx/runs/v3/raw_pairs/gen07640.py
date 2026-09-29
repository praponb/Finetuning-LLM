from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost")

result = (
    df_shipments
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "order_id", "cost")
)
