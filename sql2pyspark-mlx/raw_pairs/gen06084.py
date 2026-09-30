from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .orderBy(F.col("account_type").desc())
    .limit(50)
)
