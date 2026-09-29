from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "product_id", "quantity")
    .orderBy(F.col("product_id").desc())
)
