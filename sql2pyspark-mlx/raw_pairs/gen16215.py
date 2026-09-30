from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.is_active") & F.col("o.status").isin(["RETURNED", "PENDING"]))
    .select(F.col("o.order_date"), F.col("c.signup_date"), F.col("c.age"))
    .orderBy(F.col("order_date").desc())
)
