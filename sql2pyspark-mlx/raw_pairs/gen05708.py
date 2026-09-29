from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status")

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        F.max("amount").over(w).alias("last4_max_amount"),
        F.min("amount").over(w).alias("last4_min_amount"),
        F.sum("amount").over(w).alias("last4_sum_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)
