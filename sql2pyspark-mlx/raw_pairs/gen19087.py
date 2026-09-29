from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("credit_limit") <= 5700).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("a.account_type"))
    .orderBy(F.col("id").desc(), "account_type")
)
