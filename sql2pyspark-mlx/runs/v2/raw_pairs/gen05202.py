from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
    .orderBy("running_amount", F.col("id").desc())
)
