from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("customers.id") == F.col("accounts.customer_id"), "left_semi")
    .select(F.col("customers.id"), F.col("customers.is_active"))
)
