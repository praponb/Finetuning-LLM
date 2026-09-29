from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 4) & F.col("status").isin(["PENDING", "COMPLETED"]) & (F.year("order_date") == 2024))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        F.col("discount").cast("int").alias("discount_int"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("discount_int").desc(), "id")
    .limit(10)
)
