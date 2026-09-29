from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .select(
        "id",
        "order_id",
        "paid_at",
        F.percent_rank().over(w).alias("paid_at_pct_rank"),
    )
    .orderBy(F.col("paid_at_pct_rank").desc(), "paid_at")
)
