from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-10-01")
    .select(
        "id",
        "paid_at",
        "amount",
        "order_id",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
)
