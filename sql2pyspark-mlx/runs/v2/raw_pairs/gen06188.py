from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.row_number().over(w2).alias("paid_at_row"),
    )
    .orderBy(F.col("running_amount").desc(), "order_id")
)
