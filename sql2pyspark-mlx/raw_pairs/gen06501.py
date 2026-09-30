from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.credit_count") > 51)
    .select(
        F.col("c.signup_date"),
        F.col("c.country"),
        F.col("ct.min_opened_date"),
        F.col("ct.credit_count"),
    )
    .orderBy(F.col("credit_count").desc())
)
