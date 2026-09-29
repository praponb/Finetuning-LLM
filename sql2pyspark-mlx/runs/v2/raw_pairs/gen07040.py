from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .filter(F.col("a.customer_id").isNotNull())
    .join(df_customers.filter(F.col("email").like("%@company.co")).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.balance") < 40000)
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("a.status"))
)
