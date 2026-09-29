from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.lead("amount", 2).over(w).alias("next_amount"),
    )
    .orderBy(F.col("paid_at").desc())
)
