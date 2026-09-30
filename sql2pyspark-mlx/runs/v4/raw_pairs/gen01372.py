from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("greatest_discount_amount").asc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "customer_id").orderBy("order_date", "id")
w_recent = Window.partitionBy("status")
w5 = Window.partitionBy("store_id")

valid = (
    df_orders
    .filter(F.col("store_id").isin([4, 22, 34]))
    .withColumn("greatest_discount_amount", F.greatest("discount", "amount"))
    .withColumn("channel_status_key", F.concat_ws("-", "channel", "status"))
)

result = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("greatest_discount_amount_dense_rank"),
        F.sum("discount").over(w2).alias("running_discount"),
        F.lag("shipping_fee", 2).over(w3).alias("prev_shipping_fee"),
        F.max("shipping_fee").over(w_recent).alias("status_max_shipping_fee"),
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
    )
)
