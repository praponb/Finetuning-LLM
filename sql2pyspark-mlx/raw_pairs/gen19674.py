from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        F.round((F.col("amount") * 1.15), 2).alias("amount_scaled"),
        F.trim("status").alias("clean_status"),
    )
    .orderBy(F.col("clean_status").desc())
)
