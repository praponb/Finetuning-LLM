from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "amount",
        "status",
        "paid_at",
        F.rank().over(w).alias("amount_rank"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
    .orderBy(F.col("running_amount").desc())
)
