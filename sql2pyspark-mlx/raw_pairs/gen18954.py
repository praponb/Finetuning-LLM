from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.lower(F.trim(F.col("customers.email"))) == "bob.smith@yahoo.com")
    .select(
        F.col("orders.customer_id"),
        F.col("orders.discount"),
        F.col("customers.segment"),
    )
    .orderBy("discount", "customer_id")
)
