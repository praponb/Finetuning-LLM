from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
    .select("id", "status", "shipping_fee", "discount")
)
