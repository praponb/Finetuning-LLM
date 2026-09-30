from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "PENDING") & ((F.col("discount") < 0.17) | F.col("amount").isNotNull()))
    .select(
        "id",
        "status",
        "channel",
        F.when(F.col("status") == "RETURNED", 1).otherwise(0).alias("is_returned"),
    )
)
