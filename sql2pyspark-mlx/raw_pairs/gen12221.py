from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .filter(~F.col("customers.is_active") & (F.col("customers.segment") == "SMB"))
    .select(
        F.col("customers.signup_date"),
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
        F.regexp_replace(F.col("customers.name"), " ", "_").alias("name_slug"),
    )
)
