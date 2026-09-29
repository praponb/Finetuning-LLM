from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("method")

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2025-03-27")
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        "method",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
    .orderBy("next_amount")
)
