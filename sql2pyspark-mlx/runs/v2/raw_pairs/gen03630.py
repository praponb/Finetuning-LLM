from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 38]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("unit_price").desc(), F.col("quantity").desc())
)
