from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("status") != "REFUNDED")
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        F.lead("amount", 2).over(w).alias("next_amount"),
    )
)
