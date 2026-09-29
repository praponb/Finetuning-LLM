from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .groupBy(
        F.when(F.col("customers.credit_limit") >= 15300, "high").when(F.col("customers.credit_limit") >= 3500, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("customers.segment"),
    )
    .agg(F.min(F.col("accounts.balance")).alias("min_balance"))
    .orderBy("segment")
)
