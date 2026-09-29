from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
)
