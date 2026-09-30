from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-03-01")
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        "order_id",
        "method",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
        F.first("method").over(w2).alias("first_method"),
    )
    .orderBy("amount_change")
)
