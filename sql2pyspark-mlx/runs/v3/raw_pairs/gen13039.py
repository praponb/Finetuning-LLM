from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("amount") >= 1110)
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
    )
)
