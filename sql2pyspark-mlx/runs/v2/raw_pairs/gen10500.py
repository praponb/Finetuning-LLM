from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "FAILED"]))
    .select(
        "id",
        "status",
        "amount",
        F.avg("amount").over(w).alias("status_avg_amount"),
    )
)
