from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .select(
        "id",
        "status",
        "amount",
        F.min("amount").over(w).alias("status_min_amount"),
    )
)
