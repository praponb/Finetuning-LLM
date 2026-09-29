from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "outer")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter((F.year(F.col("stores.opened_date")) == 2024) & (F.col("customers.age") >= 59))
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("customers.email"),
        F.col("stores.store_name"),
        F.when(F.col("customers.credit_limit") >= 13400, "High").when(F.col("customers.credit_limit") >= 2500, "Medium").otherwise("Low").alias("credit_limit_tier"),
    )
)
