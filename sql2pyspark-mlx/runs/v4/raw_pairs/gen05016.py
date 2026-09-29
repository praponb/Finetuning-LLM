from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .select(
        F.col("customers.segment"),
        F.col("accounts.balance"),
        F.col("accounts.customer_id"),
        F.floor(F.col("customers.age")).alias("age_whole"),
    )
    .orderBy("age_whole")
)
