from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter((F.col("c.age") != 54) & F.col("o.order_date").between("2023-04-01", "2025-07-01"))
    .select(F.col("o.status"), F.col("o.amount"), F.col("c.country"))
)
