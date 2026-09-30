from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("amount", "id")
w4 = Window.orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)

metrics = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.ntile(3).over(w3).alias("amount_bucket"),
        F.avg("amount").over(w4).alias("amount_avg_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "amount_share",
        "cumulative_amount",
        "amount_bucket",
    )
)
