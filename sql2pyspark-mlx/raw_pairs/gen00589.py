from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        F.last("channel").over(w).alias("last_channel"),
    )
    .orderBy("order_date", "channel")
)
