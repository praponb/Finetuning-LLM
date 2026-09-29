from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id", "status")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

with_windows = (
    df_shipments
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.sum("cost").over(w3).alias("status_sum_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share_2"),
        F.avg("cost").over(w5).alias("cost_avg_6"),
    )
)

result = (
    with_windows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "event_seq",
        "cost_share",
        F.col("sum_payment_amount"),
    )
    .orderBy(F.col("cost").desc())
)
