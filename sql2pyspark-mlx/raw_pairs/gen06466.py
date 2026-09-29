from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "status",
        "order_id",
        "paid_at",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
    .orderBy(F.col("amount").desc())
)
