from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount")
w2 = Window.partitionBy("method")

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.avg("amount").over(w2).alias("method_avg_amount"),
    )
)
