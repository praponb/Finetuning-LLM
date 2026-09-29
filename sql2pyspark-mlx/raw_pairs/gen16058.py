from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("t.account_id").isin([5, 18, 27]))
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.sum(F.col("t.amount")).alias("total_amount"))
)
