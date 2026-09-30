from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status", "customer_id").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        F.count("discount").over(w_recent).alias("last4_count_discount"),
        F.avg("discount").over(w_recent).alias("last4_avg_discount"),
        F.min("discount").over(w_recent).alias("last4_min_discount"),
    )
)
