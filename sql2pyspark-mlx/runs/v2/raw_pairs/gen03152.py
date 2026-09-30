from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("unit_price") >= 230)
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
    .select("id", "product_id", "unit_price", "quantity")
    .orderBy(F.col("unit_price").desc(), F.col("id").desc())
)
