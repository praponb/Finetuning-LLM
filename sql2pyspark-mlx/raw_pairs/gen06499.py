from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type").isin(["SAVINGS", "CREDIT"]) & (F.col("opened_date") > "2024-07-01")) | F.col("customer_id").isNull())
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .orderBy(F.col("days_since_opened_date").desc(), F.col("account_type").desc())
)
