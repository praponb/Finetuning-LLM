from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost")

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "LOST", "DELIVERED"]))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "carrier", "cost", "delivered_date")
    .orderBy("delivered_date")
)
