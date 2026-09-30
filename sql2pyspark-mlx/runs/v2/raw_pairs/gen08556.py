from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.hour("paid_at").between(10, 14))
    .groupBy("method")
    .agg(
        F.count("amount").alias("amount_count"),
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
    .filter(F.col("max_amount") >= 1310)
    .orderBy("method")
)
