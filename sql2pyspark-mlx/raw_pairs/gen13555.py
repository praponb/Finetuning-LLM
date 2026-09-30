from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
        F.col("customers.credit_limit"),
        F.col("customers.city"),
        F.col("customers.email"),
        F.regexp_extract(F.col("customers.email"), "@(.+)$", 1).alias("email_domain"),
    )
    .orderBy(F.col("opened_date").desc())
)
