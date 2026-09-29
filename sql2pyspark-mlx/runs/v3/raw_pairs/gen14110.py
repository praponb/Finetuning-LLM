from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(710, 1180))
    .select(
        "store_id",
        "order_date",
        "shipping_fee",
        F.lower("status").alias("status_lower"),
    )
)
