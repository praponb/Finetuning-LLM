from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .select(
        "customer_id",
        "discount",
        "channel",
        (F.col("amount") - F.col("discount")).alias("amount_minus_discount"),
        F.year("order_date").alias("order_date_year"),
    )
)
