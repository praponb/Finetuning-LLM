from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.upper("email").like("%ENR%")).select("id")

result = (
    df_orders.alias("o")
    .join(excluded_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("o.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.channel"))
)
