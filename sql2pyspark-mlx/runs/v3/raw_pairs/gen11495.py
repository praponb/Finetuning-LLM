from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 10)
    .select(
        "id",
        "amount",
        F.when(F.col("status") == "SHIPPED", 1).otherwise(0).alias("is_shipped"),
    )
)
