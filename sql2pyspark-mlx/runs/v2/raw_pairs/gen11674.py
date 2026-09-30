from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("accounts.status"))
    .agg(
        F.max(F.col("transactions.amount")).alias("max_amount"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc(), F.col("max_amount").desc())
)
