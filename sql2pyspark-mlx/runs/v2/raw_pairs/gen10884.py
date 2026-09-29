from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.sum(F.when(F.col("t.merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
    )
)
