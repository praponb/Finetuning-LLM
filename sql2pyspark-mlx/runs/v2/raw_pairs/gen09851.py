from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("customers.segment"),
        F.col("customers.name"),
        F.col("customers.country"),
        F.col("accounts.balance"),
        F.col("accounts.opened_date"),
    )
    .orderBy("country")
)
