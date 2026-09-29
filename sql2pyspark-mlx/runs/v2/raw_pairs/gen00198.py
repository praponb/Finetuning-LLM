from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .select(
        "id",
        "order_id",
        "amount",
        "method",
        "paid_at",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.first("order_id").over(w2).alias("first_order_id"),
    )
)
