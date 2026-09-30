from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-08-01", "2025-08-01"))
    .select("id", "customer_id")
    .unionByName(df_orders.filter(F.col("discount") > 0.25).select("id", "customer_id"))
)
