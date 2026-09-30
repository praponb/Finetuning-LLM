from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("channel").orderBy("discount", "id")
w_running = Window.orderBy(F.col("amount").desc(), "id")

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.discount"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.order_date"),
        F.col("b.discount").alias("order_discount"),
        F.col("b.channel"),
    )
)

joined_payments = (
    joined
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.order_date").alias("order_order_date"),
        F.col("o.status").alias("order_status"),
    )
)

result = (
    joined_payments
    .withColumn("discount_bucket", F.ntile(4).over(w_recent))
    .withColumn("amount_bucket", F.ntile(4).over(w_running))
)
