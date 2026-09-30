from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "amount",
        F.ntile(10).over(w).alias("paid_at_ntile"),
        F.percent_rank().over(w2).alias("amount_pct_rank"),
    )
)
