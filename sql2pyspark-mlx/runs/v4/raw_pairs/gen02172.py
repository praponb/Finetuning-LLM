from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .filter(F.col("status") != "RETURNED")
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "carrier", "cost", "delivered_date")
    .orderBy(F.col("delivered_date").desc())
)
