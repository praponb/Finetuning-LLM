from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        F.count("shipping_fee").over(w).alias("running_count_shipping_fee"),
        F.sum("shipping_fee").over(w).alias("running_sum_shipping_fee"),
        F.avg("shipping_fee").over(w).alias("running_avg_shipping_fee"),
        F.max("shipping_fee").over(w).alias("running_max_shipping_fee"),
    )
)
