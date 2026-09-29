from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.orderBy("amount", "id")

scored = (
    df_payments
    .filter(F.col("paid_at") >= "2024-09-01")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)

result = (
    scored
    .select("id", "order_id", "amount", "status", "next_amount")
    .orderBy(F.col("order_id").desc_nulls_first())
)
