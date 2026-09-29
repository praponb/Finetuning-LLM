from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(F.sum(F.col("a.balance")).alias("total_balance"))
    .filter(F.col("total_balance") > 121000)
    .orderBy("segment", F.col("total_balance").desc())
)
