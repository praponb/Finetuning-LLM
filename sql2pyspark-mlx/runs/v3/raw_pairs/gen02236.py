from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.countDistinct("balance").alias("unique_account_types"),
        F.max("balance").alias("max_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.unique_account_types") > 7)
    .select(
        F.col("c.segment"),
        F.col("c.signup_date"),
        F.col("cs.unique_account_types"),
        F.col("cs.max_balance"),
    )
    .orderBy(F.col("segment").desc())
)
