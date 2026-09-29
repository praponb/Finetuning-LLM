from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
    .orderBy("amount")
)
