from pyspark.sql import functions as F

base = df_accounts.filter(~((F.col("customer_id") == 34) & F.col("status").isNotNull()))

summary = (
    base
    .groupBy("status")
    .agg(F.countDistinct("balance").alias("unique_account_types"))
)

result = (
    summary
    .filter(F.col("unique_account_types") > 10)
    .select("status", "unique_account_types")
    .orderBy(F.col("unique_account_types").desc())
)
