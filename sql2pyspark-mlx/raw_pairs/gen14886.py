from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "order_date"), "channel").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy(F.month("order_date"), "customer_id").orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("order_date") < "2023-04-10")
    .select(
        "id",
        "customer_id",
        "status",
        "shipping_fee",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_2"),
        F.ntile(4).over(w2).alias("shipping_fee_bucket"),
    )
)
