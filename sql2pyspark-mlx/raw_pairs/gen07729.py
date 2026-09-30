from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "discount",
        "shipping_fee",
        F.count("shipping_fee").over(w_running).alias("running_count_shipping_fee"),
        F.min("shipping_fee").over(w_running).alias("running_min_shipping_fee"),
        F.avg("shipping_fee").over(w_running).alias("running_avg_shipping_fee"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "running_count_shipping_fee",
        "running_min_shipping_fee",
        "running_avg_shipping_fee",
    )
    .orderBy(F.col("running_min_shipping_fee").desc_nulls_last())
)
