from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("order_id").isNull() & (F.col("amount") < 1470) & F.col("order_id").isNull()))
    .select("order_id", "status", F.substring("method", 1, 3).alias("method_prefix"))
)
