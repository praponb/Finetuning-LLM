from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("account_type")).alias("account_type_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.country"), F.col("cs.account_type_list"))
    .orderBy(F.col("country").desc())
)
