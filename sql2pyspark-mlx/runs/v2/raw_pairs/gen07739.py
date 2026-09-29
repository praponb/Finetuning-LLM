from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "COMPLETED") & (F.col("shipping_fee") < 29))
    .select(
        "id",
        "store_id",
        "amount",
        "shipping_fee",
        F.trim("status").alias("clean_status"),
    )
)
