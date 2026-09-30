from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .select(
        "id",
        "method",
        "status",
        "paid_at",
        "order_id",
        F.row_number().over(w).alias("rn"),
        F.first("method").over(w2).alias("first_method"),
    )
)
