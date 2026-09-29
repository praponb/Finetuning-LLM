from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("paid_at"), "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

with_windows = (
    df_payments
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("amount_bucket", F.ntile(3).over(w2))
    .withColumn("prev_amount", F.lag("amount", 1).over(w3))
)

result = (
    with_windows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_bucket",
        "prev_amount",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
