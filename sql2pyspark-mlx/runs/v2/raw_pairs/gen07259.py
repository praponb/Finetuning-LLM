from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "amount",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
    )
)
