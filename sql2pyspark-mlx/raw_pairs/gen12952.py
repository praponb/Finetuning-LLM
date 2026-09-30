from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy(F.month("paid_at"), "method").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        "method",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.row_number().over(w2).alias("row_num"),
    )
)
