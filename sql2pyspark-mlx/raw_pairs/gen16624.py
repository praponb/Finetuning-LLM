from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "paid_at"), "order_id")
w2 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("paid_at").desc(), "id")

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-09-24 14:00:00")))
    .select(
        "*",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.quarter("paid_at").alias("paid_at_quarter"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

result = (
    valid
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "status_prefix",
        F.min("amount").over(w).alias("paid_at_period_min_amount"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)
