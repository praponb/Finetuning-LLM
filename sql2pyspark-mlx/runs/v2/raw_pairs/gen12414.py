from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("customers.credit_limit") > 10600) & F.col("customers.credit_limit").isNull())
    .select(
        F.col("accounts.status"),
        F.col("accounts.opened_date"),
        F.col("accounts.balance"),
        F.col("customers.city"),
        F.col("customers.age"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("city").desc())
)
