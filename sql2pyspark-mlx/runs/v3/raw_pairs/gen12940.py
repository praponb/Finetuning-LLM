from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .filter(F.col("status") != "DELIVERED")
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "order_id", "cost")
    .orderBy(F.col("id").desc())
)
