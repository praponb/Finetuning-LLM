from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("customer_id").isNull()).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
    .filter(F.col("c.credit_limit").isNotNull())
    .select(F.col("c.id"), F.col("c.name"), F.col("c.email"), F.col("c.is_active"))
    .orderBy("email")
)
