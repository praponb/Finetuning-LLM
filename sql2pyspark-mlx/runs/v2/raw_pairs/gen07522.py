from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("city") != "London").alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.customer_id").isNull())
    .select(F.col("a.id"), F.col("a.status"), F.col("a.account_type"))
)
