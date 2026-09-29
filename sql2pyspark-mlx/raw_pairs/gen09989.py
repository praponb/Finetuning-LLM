from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "paid_at",
        "method",
        "amount",
        F.when(F.col("amount") >= 1330, "High").when(F.col("amount") >= 670, "Medium").otherwise("Low").alias("amount_level"),
    )
    .orderBy(F.col("amount").desc(), "method")
)
