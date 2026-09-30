from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "order_id",
        F.avg("amount").over(w).alias("method_avg_amount"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("order_id").desc())
)
