from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("stores.sqft") != 12367)
    .select(
        F.col("orders.store_id"),
        F.col("orders.amount"),
        F.col("stores.region"),
        F.col("customers.credit_limit"),
        F.col("customers.name"),
    )
    .orderBy(F.col("store_id").desc())
)
