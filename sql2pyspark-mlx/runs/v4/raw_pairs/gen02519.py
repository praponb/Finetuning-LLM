from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w_running = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_last())
w4 = Window.partitionBy("order_id", "product_id")
w_running_2 = Window.orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .withColumn("quantity_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("unit_price_percent_rank", F.percent_rank().over(w_running))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w4)), 4))
    .withColumn("unit_price_bucket", F.ntile(5).over(w_running_2))
)
