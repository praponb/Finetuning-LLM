from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1320) & (F.col("status") == "REFUNDED"))
    .select(
        "id",
        "amount",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
    )
)
