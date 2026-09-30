from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("balance").between(49000, 50000))
    .groupBy("customer_id")
    .agg(
        F.countDistinct("balance").alias("unique_account_types"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(
        F.col("c.is_active"),
        F.col("ct.unique_account_types"),
        F.col("ct.latest_opened_date"),
    )
)
