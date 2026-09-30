from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")
w2 = Window.partitionBy("method", "status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .filter(F.col("order_id") == 20)
    .select(
        "id",
        "order_id",
        "paid_at",
        "method",
        "status",
        "amount",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
    .orderBy("amount")
)
