from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("method") == "paypal") & ((F.col("order_id") == 21) | (F.col("amount") <= 1210)))
)

agg = (
    base
    .groupBy("method")
    .agg(F.count("*").alias("row_count"))
)

result = (
    agg
    .filter(F.col("row_count") < 5)
    .select("method", "row_count")
)
