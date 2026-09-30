from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-08-01")
    .select(
        "id",
        "status",
        "paid_at",
        F.dense_rank().over(w).alias("paid_at_dense_rank"),
    )
)
