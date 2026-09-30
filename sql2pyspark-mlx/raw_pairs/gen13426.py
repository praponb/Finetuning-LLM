from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .filter(F.col("order_id").isin([22, 26]))
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "status", "delivered_date")
)
