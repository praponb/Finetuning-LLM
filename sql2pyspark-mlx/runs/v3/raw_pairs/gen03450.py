from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.customer_id").isin([5, 11, 29, 32]))
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.max(F.col("a.balance")).alias("max_balance"))
    .orderBy("max_balance")
)
