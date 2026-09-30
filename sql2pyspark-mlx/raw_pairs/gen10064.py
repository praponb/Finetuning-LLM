from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id", "status")
    .agg(
        F.min("balance").alias("min_balance"),
        F.min("opened_date").alias("first_opened_date"),
        F.max("opened_date").alias("last_opened_date"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
    .filter(F.col("closed_count") >= 4)
    .drop("closed_count")
    .orderBy(F.col("last_opened_date").desc())
)
