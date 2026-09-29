from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("credit_limit").between(11700, 13300)).alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("o.status") != "CANCELLED")
    .select(F.col("o.id"), F.col("o.channel"))
    .orderBy(F.col("id").desc())
)
