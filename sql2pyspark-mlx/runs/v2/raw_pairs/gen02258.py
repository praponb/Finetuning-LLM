from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "PENDING") & F.col("channel").isNull() & (F.col("shipping_fee") > 18))
    .select(
        "id",
        "status",
        "store_id",
        "customer_id",
        F.when(F.col("status") == "RETURNED", 1).otherwise(0).alias("is_returned"),
    )
)
