from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(34000, 77000))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"), "status")
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy("opened_month", "status")
    .limit(20)
)
