from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & (F.col("account_type") == "CREDIT"))
    .withColumn("account_type_status", F.concat("account_type", F.lit("/"), "status"))
)

account_type_summary = (
    filtered_accounts
    .groupBy("account_type")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
    )
)

result = (
    account_type_summary
    .filter(F.col("loan_count") >= 40)
    .select("account_type", "latest_opened_date", "loan_count")
    .orderBy(F.col("loan_count").desc())
)
