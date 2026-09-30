from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter((F.col("c.segment") == "SMB") & (F.col("c.signup_date") < "2024-03-01"))
    .select(F.col("o.store_id"), F.col("o.channel"), F.col("o.status"), F.col("c.age"))
)
