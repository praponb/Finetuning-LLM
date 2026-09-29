from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("age") != 32).select("id")

result = (
    df_orders.alias("o")
    .join(excluded_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("o.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(F.col("o.id"), F.col("o.discount"), F.col("o.shipping_fee"))
    .orderBy(F.col("shipping_fee").desc())
)
