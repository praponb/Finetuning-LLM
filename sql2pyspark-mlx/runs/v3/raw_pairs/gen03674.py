from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.customer_id").isin([2, 6]) & F.col("c.segment").isin(["SMB", "Consumer", "Corporate"]))
    .select(
        F.col("c.is_active"),
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("o.store_id"),
    )
)
