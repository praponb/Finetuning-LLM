from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "customer_id", "shipping_fee", "status")
)
