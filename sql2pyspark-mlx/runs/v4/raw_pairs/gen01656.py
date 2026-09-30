from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 2)
    .orderBy("id")
)
