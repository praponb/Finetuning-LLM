from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("method", "status").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        F.ntile(3).over(w).alias("amount_bucket"),
        F.sum("amount").over(w2).alias("status_sum_amount"),
        F.rank().over(w3).alias("amount_rank"),
    )
)
