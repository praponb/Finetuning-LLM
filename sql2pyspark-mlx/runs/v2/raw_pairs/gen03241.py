from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .select(
        "id",
        "method",
        "paid_at",
        F.dense_rank().over(w).alias("paid_at_dense_rank"),
    )
)
