from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("c.age"),
        F.col("c.is_active"),
        F.concat_ws("-", F.col("a.status"), F.col("a.account_type")).alias("status_account_type_key"),
    )
)
