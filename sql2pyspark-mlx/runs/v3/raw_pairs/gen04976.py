from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("order_date") <= "2025-09-01").alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(~F.col("c.email").like("%@outlook.com"))
    .select(F.col("c.id"), F.col("c.is_active"), F.col("c.email"))
    .orderBy(F.col("email").desc(), "id")
)
