from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        F.length("status").alias("status_length"),
        F.when(F.col("method") == "paypal", 1).otherwise(0).alias("is_paypal"),
    )
    .orderBy("status_length")
)
