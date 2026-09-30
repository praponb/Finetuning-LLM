from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("amount", "id")
w3 = Window.partitionBy(F.date_trunc("MONTH", "paid_at")).orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("amount").between(500, 950))
    .select(
        "id",
        "method",
        "amount",
        "status",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
