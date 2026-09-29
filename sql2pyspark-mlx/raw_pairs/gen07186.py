from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~(F.col("c.credit_limit").between(4800, 8500) & (F.length(F.col("c.name")) < 9)))
    .select(
        F.col("s.start_date"),
        F.col("s.plan"),
        F.col("s.customer_id"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.col("c.segment"),
    )
)
