from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.signup_date") > "2024-09-01") & F.col("t.is_flagged"))
    .select(
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("a.account_type"),
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.initcap(F.trim(F.col("t.merchant"))).alias("merchant_title"),
    )
    .orderBy("merchant_title", "account_id")
)
