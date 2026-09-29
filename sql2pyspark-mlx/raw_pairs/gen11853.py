from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .orderBy(F.col("quantity").desc())
)
