from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") < 45)
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.avg(F.col("a.balance")).alias("avg_balance"))
    .orderBy("signup_quarter", F.col("avg_balance").desc())
)
