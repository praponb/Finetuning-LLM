from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile"]) | (F.col("status") == "COMPLETED"))
    .select(
        "store_id",
        "shipping_fee",
        F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"),
    )
)
