from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.credit_limit"),
        F.col("customers.segment"),
        F.col("t.account_type_list"),
        F.col("t.row_count"),
    )
    .orderBy("segment", F.col("credit_limit").desc())
)
