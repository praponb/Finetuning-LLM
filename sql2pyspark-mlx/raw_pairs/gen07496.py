from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
    .orderBy(F.col("running_amount").desc(), "paid_at")
)
