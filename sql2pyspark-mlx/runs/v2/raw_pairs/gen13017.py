from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")

result = (
    df_shipments
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "status", "cost", "delivered_date")
    .orderBy("delivered_date", "id")
)
