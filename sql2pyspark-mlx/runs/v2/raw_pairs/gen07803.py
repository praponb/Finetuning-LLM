from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_payments
    .filter(F.col("method") != "card")
    .select(
        "id",
        "status",
        "amount",
        F.sum("amount").over(w).alias("status_sum_amount"),
    )
)
