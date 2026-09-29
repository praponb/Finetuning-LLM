from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-04-01")
    .select(
        "id",
        "method",
        "amount",
        "status",
        "paid_at",
        F.rank().over(w).alias("rnk"),
        F.rank().over(w2).alias("rnk_2"),
    )
    .orderBy("paid_at")
)
