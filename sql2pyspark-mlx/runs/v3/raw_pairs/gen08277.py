from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2023-06-01")
    .select(
        "id",
        "status",
        "method",
        "paid_at",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        (F.col("amount") - F.lag("amount", 2).over(w2)).alias("amount_change"),
    )
    .orderBy(F.col("amount_change").desc(), F.col("id").desc())
)
