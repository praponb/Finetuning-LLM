from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id").isNotNull() & F.col("method").isNotNull()) | F.col("method").isin(["bank_transfer", "cash", "card"]))
    .select(
        "method",
        F.when(F.col("method") == "paypal", 1).otherwise(0).alias("is_paypal"),
    )
    .orderBy("method")
)
