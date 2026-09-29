from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.country") == "SG")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("c.country"),
        F.col("c.credit_limit"),
    )
)
