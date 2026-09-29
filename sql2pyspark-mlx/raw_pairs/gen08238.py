from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_orders
    .filter(F.col("discount").between(0.1, 0.34))
    .select(
        "id",
        "status",
        "channel",
        "shipping_fee",
        F.lag("discount", 1).over(w).alias("prev_discount"),
        F.avg("discount").over(w2).alias("discount_avg_3"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
    )
)

result = (
    metrics
    .select(
        "id",
        "shipping_fee",
        "prev_discount",
        "discount_avg_3",
        "cumulative_amount",
    )
    .orderBy(F.col("shipping_fee").asc_nulls_last(), "id")
)
