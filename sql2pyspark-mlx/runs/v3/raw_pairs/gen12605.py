from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") > "2024-06-30") & (F.col("status") == "PENDING") & F.col("status").isin(["PENDING", "SHIPPED"]))
    .select(
        "id",
        "store_id",
        "status",
        "customer_id",
        F.round((F.col("discount") * 0.9), 2).alias("adjusted_discount"),
    )
    .orderBy(F.col("id").desc())
)
