from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("method", "order_id")
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-2, -1)
w_running = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.avg("amount").over(w3).alias("amount_avg_2_prior"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)
