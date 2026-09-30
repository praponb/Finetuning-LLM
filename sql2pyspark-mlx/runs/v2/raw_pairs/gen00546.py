from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "CREDIT"]))
    .groupBy("customer_id")
    .agg(F.countDistinct("balance").alias("unique_customers"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .filter(F.col("customer_stats.unique_customers") > 6)
    .select(
        F.col("customers.signup_date"),
        F.col("customers.email"),
        F.col("customer_stats.unique_customers"),
    )
)
