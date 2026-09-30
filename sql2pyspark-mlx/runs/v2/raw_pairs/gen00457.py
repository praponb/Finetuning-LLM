from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")
w2 = Window.partitionBy("order_id", "method")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "method",
        "amount",
        F.rank().over(w).alias("rnk"),
        F.avg("amount").over(w2).alias("order_id_avg_amount"),
    )
)
