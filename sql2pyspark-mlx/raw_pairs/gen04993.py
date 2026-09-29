from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.year(F.col("customers.signup_date")).alias("signup_year"))
    .agg(
        F.max(F.col("accounts.opened_date")).alias("latest_opened_date"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("latest_opened_date").desc())
)
