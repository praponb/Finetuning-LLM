from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id", "method")

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "amount",
        F.row_number().over(w).alias("rn"),
        F.min("amount").over(w2).alias("order_id_min_amount"),
    )
)
