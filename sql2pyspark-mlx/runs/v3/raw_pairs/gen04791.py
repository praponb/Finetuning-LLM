from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "status", "shipped_date", "order_id")
    .orderBy(F.col("order_id").desc())
)
