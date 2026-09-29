from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
    )
    .orderBy("amount")
)
