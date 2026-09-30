from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("A"), F.col("c.id") == F.col("A.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.col("A.total_count"),
        F.col("A.first_opened_date"),
    )
    .orderBy("total_count")
)
