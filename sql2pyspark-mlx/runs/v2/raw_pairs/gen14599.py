from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("discount").between(0.25, 0.28))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "customer_id", "shipping_fee", "store_id")
)
