from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "channel").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        F.min("discount").over(w).alias("running_min_discount"),
        F.count("discount").over(w).alias("running_count_discount"),
        F.sum("discount").over(w).alias("running_sum_discount"),
        F.lag("discount", 1).over(w2).alias("prev_discount"),
    )
)
