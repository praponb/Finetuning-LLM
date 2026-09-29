from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("quantity").between(7, 8))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy("order_id")
)
