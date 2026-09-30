from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .filter(F.col("accounts.customer_id").isNotNull())
    .join(df_customers.filter(F.col("credit_limit").isNull()).alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_anti")
    .select(F.col("accounts.id"), F.col("accounts.status"))
    .orderBy(F.col("id").desc())
)
