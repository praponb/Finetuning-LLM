from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("amount") > 1450)
    .select(
        "id",
        "method",
        "status",
        "paid_at",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
    )
)
