from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "amount",
        F.percent_rank().over(w).alias("paid_at_pct_rank"),
        F.lag("amount", 2).over(w2).alias("prev_amount"),
    )
)
