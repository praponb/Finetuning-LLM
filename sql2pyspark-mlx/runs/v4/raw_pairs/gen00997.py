from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.filter(F.col("segment").isin(["SMB", "Corporate", "Consumer"])).alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_semi")
    .select(F.col("accounts.id"), F.col("accounts.opened_date"))
)
