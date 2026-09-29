from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("order_id").orderBy(F.col("is_refunded").desc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy(F.col("paid_at_date").desc(), "id")

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.to_date("paid_at").alias("paid_at_date"),
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
    )
)

result = (
    base
    .filter(~F.col("method").isin(["cash", "promptpay"]))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at_date",
        "is_refunded",
        F.avg("amount").over(w).alias("amount_avg_5_prior"),
        F.cume_dist().over(w2).alias("is_refunded_cume_dist"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
