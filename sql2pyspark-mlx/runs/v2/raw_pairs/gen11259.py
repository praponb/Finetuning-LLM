from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.filter(F.col("city") == "New York").alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_semi")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.customer_id"),
    )
    .orderBy("customer_id")
)
