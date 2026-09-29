from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.segment") == "Consumer")
    .groupBy(F.col("customers.segment"), F.col("customers.city"))
    .agg(F.max(F.col("accounts.opened_date")).alias("last_opened_date"))
    .orderBy("segment", F.col("last_opened_date").desc(), "city")
    .limit(10)
)
