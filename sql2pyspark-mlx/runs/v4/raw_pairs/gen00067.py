from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("quantity", "id")
w_recent = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").asc_nulls_first())
w3 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .withColumn("quantity_bucket", F.ntile(5).over(w_ordered))
    .withColumn("quantity_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("quantity_bucket_2", F.ntile(5).over(w3))
)
