from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.countDistinct(F.col("a.balance")).alias("unique_account_types"),
        F.count("*").alias("num_accounts"),
    )
    .orderBy("unique_account_types")
)
