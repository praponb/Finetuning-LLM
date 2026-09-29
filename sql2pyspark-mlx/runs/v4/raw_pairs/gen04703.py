from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_status", "order_id")
w2 = Window.partitionBy("method")
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("method_count_amount_filled", "id")
w5 = Window.partitionBy("order_status").orderBy("amount_max_7d", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("order_status", "status").orderBy("amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method", "order_id").orderBy("order_order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("amount") < 1270)
    .select("*", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
)

joined = (
    valid
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.status").alias("order_status"),
        F.col("d.order_date"),
        F.col("d.amount").alias("order_amount"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.min("amount_filled").over(w_grp).alias("order_status_min_amount_filled"),
        F.count("amount_filled").over(w2).alias("method_count_amount_filled"),
        F.max("amount").over(w3).alias("amount_max_7d"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.order_date").alias("order_order_date"))
)

scored = (
    with_dims
    .withColumn("prev_amount", F.lag("amount", 1).over(w4))
    .withColumn("order_status_min_amount_filled_avg_6", F.avg("order_status_min_amount_filled").over(w5))
    .withColumn("last_known_amount_max_7d", F.last("amount_max_7d", ignorenulls=True).over(w6))
    .withColumn("running_order_amount", F.sum("order_amount").over(w7))
)

with_dims_payments = (
    scored
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select("e.*", F.col("o.shipping_fee"), F.col("o.status").alias("order_status_2"))
)

result = (
    with_dims_payments
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "amount_filled",
        "order_date",
        "order_amount",
        "order_status_min_amount_filled",
        "order_order_date",
    )
)
