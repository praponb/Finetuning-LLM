from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("amount")
w2 = Window.partitionBy("order_id", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-06-01 12:00:00").cast("timestamp")) & (F.col("amount") < 1530))
    .select("*", F.initcap(F.trim("method")).alias("method_title"))
)

result = (
    base
    .select(
        "id",
        "method",
        "amount",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)
