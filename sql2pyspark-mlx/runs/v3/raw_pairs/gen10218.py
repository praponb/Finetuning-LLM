from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
    .orderBy(F.col("paid_at").desc())
)
