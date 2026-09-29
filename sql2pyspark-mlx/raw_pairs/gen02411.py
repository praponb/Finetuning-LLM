from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee")

result = (
    df_orders
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
)
