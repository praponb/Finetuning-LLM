from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment").isin(["Corporate", "SMB", "Consumer"]) & (F.col("s.plan") == "standard"))
    .select(
        F.col("c.segment"),
        F.col("s.customer_id"),
        F.col("s.plan"),
        F.when(F.col("c.country") == "UK", 1).otherwise(0).alias("is_uk"),
    )
)
