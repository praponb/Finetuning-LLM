from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("account_type").isin(["LOAN", "SAVINGS"])).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .filter(F.col("c.signup_date") <= "2023-06-16")
    .select(F.col("c.id"), F.col("c.age"))
)
