from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.city") == "Bangkok")
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("c.is_active"),
        F.round((F.col("c.age") * 1.15), 2).alias("adjusted_age"),
    )
)
