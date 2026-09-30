from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        "status",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
        F.lag("amount", 2).over(w2).alias("prev_amount"),
    )
)
