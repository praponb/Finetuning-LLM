from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("discount")
w_part = Window.partitionBy("store_id", "customer_id").orderBy(F.col("discount").desc())
w3 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .withColumn("discount_rank", F.rank().over(w))
    .withColumn("discount_cume_dist", F.cume_dist().over(w_part))
    .withColumn("shipping_fee_bucket", F.ntile(3).over(w3))
)
