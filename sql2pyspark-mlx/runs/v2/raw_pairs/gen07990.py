from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("method") == "paypal")
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        F.row_number().over(w).alias("rn"),
        F.sum("amount").over(w).alias("running_amount"),
    )
)
