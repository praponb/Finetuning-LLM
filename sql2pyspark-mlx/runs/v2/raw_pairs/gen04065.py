from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "unit_price", "quantity")
    .orderBy(F.col("id").desc())
)
