from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc())
w2 = Window.partitionBy("method").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "status",
        "paid_at",
        "method",
        F.rank().over(w).alias("paid_at_rank"),
        F.rank().over(w2).alias("paid_at_rank_2"),
    )
)
