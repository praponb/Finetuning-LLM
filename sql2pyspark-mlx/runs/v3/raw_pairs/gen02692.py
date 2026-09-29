from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(980, 1220))
    .select(
        "order_id",
        "method",
        F.when(F.col("amount") >= 1480, "High").when(F.col("amount") >= 320, "Medium").otherwise("Low").alias("amount_level"),
    )
)
