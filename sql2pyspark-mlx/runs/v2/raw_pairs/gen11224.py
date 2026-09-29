from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("opened_date") > "2025-01-01") & (F.col("customer_id").isin([1, 11]) | F.month("opened_date").isin([1, 3, 10])))
)

account_type_summary = (
    src
    .groupBy("account_type", "status")
    .agg(F.count("*").alias("account_count"))
)

result = (
    account_type_summary
    .filter(F.col("account_count") > 5)
    .select("account_type", "account_count")
    .orderBy(F.col("account_count").desc(), "account_type")
)
