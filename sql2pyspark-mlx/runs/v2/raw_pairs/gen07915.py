from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("customers.name"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("name").desc())
)
