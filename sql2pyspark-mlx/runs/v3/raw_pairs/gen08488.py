from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("product_id") == 37)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "order_id", "quantity", "product_id")
    .orderBy(F.col("order_id").desc())
)
