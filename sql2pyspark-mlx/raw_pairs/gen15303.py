from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")
discount_by_store_id = df_orders.groupBy("store_id").agg(F.min("discount").alias("min_order_discount"))

with_windows = (
    df_orders
    .select(
        "*",
        F.sum("shipping_fee").over(w).alias("running_sum_shipping_fee"),
        F.avg("shipping_fee").over(w).alias("running_avg_shipping_fee"),
        F.count("shipping_fee").over(w).alias("running_count_shipping_fee"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    with_windows
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "running_avg_shipping_fee",
        F.col("min_order_discount"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
