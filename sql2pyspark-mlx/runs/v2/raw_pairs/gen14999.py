from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("accounts.customer_id").isin([2, 7, 32]))
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("customers.signup_date"),
        F.col("customers.name"),
    )
    .orderBy(F.col("name").desc(), "id")
    .limit(10)
)
