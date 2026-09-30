from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount")
w_running = Window.partitionBy("status", "customer_id")
w4 = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("status", "store_id").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_orders
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.cume_dist().over(w2).alias("discount_cume_dist"),
        F.count("discount").over(w_running).alias("status_count_discount"),
        F.avg("shipping_fee").over(w4).alias("shipping_fee_avg_3"),
        F.avg("amount").over(w5).alias("amount_avg_2"),
    )
)
