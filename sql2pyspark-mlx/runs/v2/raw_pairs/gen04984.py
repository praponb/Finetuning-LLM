from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .filter(F.col("a.customer_id").isNotNull())
    .join(df_customers.filter(~F.col("name").like("%Ivy%")).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("a.balance"))
    .orderBy(F.col("id").desc())
)
