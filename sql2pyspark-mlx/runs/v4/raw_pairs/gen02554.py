from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.is_active"))
    .agg(
        F.countDistinct(F.col("accounts.balance")).alias("unique_statuss"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("accounts.status")))).alias("status_values"),
    )
)
