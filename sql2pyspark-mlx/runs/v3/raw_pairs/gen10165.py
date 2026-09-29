from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select(
        "id",
        "order_id",
        "amount",
        F.sum("amount").over(w).alias("order_id_sum_amount"),
        F.min("amount").over(w).alias("order_id_min_amount"),
    )
)
