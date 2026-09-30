from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-09-01", "2024-10-01"))
    .select("id", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("discount") <= 0.34)
        .select("id", "customer_id")
    )
)
