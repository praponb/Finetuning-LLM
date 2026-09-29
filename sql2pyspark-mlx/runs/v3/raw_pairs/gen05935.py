from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .groupBy(F.col("customers.segment"), F.col("customers.is_active"))
    .agg(F.sort_array(F.collect_set(F.col("accounts.status"))).alias("status_list"))
    .orderBy("segment", "is_active")
    .limit(20)
)
