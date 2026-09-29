from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("status", "method").orderBy("paid_at")

result = (
    df_payments
    .filter(~F.col("method").isin(["paypal", "cash"]))
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy("method")
)
