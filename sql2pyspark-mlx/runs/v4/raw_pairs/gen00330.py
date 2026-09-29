from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.id"), F.col("customers.email"))
    .agg(
        F.count(F.col("accounts.id")).alias("account_count"),
        F.max(F.col("accounts.opened_date")).alias("latest_opened_date"),
        F.sort_array(F.collect_set(F.col("accounts.status"))).alias("status_list"),
    )
    .orderBy(F.col("latest_opened_date").desc())
)
