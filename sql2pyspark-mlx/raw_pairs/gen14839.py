from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment") != "Corporate")
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .filter(F.col("credit_count") > 48)
    .orderBy("signup_month")
    .limit(3)
)
