from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "UPS")
    .select(
        "order_id",
        F.when(F.col("status") == "DELIVERED", 1).otherwise(0).alias("is_delivered"),
    )
)
