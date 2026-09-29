from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount")

result = (
    df_payments
    .filter(F.col("amount") <= 1020)
    .select(
        "id",
        "order_id",
        "amount",
        F.dense_rank().over(w).alias("amount_dense_rank"),
    )
)
