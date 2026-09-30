from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.country").isin(["US", "SG"]))
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("c.city"),
        F.col("c.country"),
    )
)
