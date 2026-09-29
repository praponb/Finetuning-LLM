from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        F.when(F.col("amount") >= 1700, "High").when(F.col("amount") >= 670, "Medium").otherwise("Low").alias("amount_level"),
    )
    .orderBy(F.col("amount").desc())
)
