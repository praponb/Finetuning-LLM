from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.length(F.col("customers.name")) > 8)
    .groupBy(F.col("customers.country"))
    .agg(F.sum(F.col("accounts.balance")).alias("total_balance"))
    .orderBy("country")
)
