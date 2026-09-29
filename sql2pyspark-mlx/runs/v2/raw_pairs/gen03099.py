from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "order_id",
        F.first("order_id").over(w).alias("first_order_id"),
    )
    .orderBy(F.col("first_order_id").desc())
)
