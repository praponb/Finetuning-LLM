from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at")

result = (
    df_payments
    .select(
        "id",
        "method",
        "status",
        "paid_at",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
        F.percent_rank().over(w2).alias("paid_at_pct_rank"),
    )
)
