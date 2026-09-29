from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([16, 17, 30, 38]) & F.col("opened_date").between("2023-05-06", "2023-06-01"))
    .groupBy("customer_id", F.year("opened_date").alias("opened_year"))
    .agg(F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"))
)
