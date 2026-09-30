from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "order_id", "delivered_date")
    .orderBy(F.col("delivered_date").desc())
)
