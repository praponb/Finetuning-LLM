from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("method")
w4 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w_part = Window.partitionBy(F.date_trunc("MONTH", "paid_at"), "method").orderBy("paid_at", "id").rowsBetween(-6, -1)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

with_windows = (
    df_payments
    .select(
        "*",
        F.ntile(10).over(w).alias("amount_bucket"),
        F.lag("amount", 1, 0).over(w2).alias("prev_amount"),
        F.max("amount").over(w3).alias("method_max_amount"),
        F.avg("amount").over(w4).alias("amount_avg_3"),
        F.avg("amount").over(w_part).alias("amount_avg_6_prior"),
    )
)

result = (
    with_windows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "amount_bucket",
        "prev_amount",
        "method_max_amount",
        "amount_avg_3",
        F.col("avg_payment_amount"),
    )
)
