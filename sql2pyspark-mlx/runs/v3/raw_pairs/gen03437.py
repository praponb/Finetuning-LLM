from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment").isin(["SMB", "Corporate", "Consumer"]))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("c.signup_date"),
        F.col("c.name"),
        F.col("c.segment"),
    )
)
