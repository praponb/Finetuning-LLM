from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("stores.opened_date").between("2023-09-01", "2025-10-20") & (F.col("orders.amount") >= 1280))
    .select(
        F.col("orders.amount"),
        F.col("customers.credit_limit"),
        F.col("stores.city"),
        F.col("stores.region"),
    )
)
