from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("city").isin(["Bangkok", "Chiang Mai"])).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
)
