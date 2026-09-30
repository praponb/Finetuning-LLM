from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.orderBy(F.col("amount_int").desc_nulls_last())

base = (
    df_payments
    .filter((F.col("method") == "paypal") & F.col("order_id").isNotNull())
    .select("id", "method", "amount", F.col("amount").cast("int").alias("amount_int"))
)

result = (
    base
    .select(
        "id",
        "method",
        F.rank().over(w).alias("amount_rank"),
        F.cume_dist().over(w2).alias("amount_int_cume_dist"),
    )
)
