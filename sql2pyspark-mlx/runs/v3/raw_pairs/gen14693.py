from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"), "customer_id")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.avg("balance").alias("avg_balance"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
    .filter(F.col("closed_count") >= 3)
    .orderBy("opened_year", F.col("customer_id").desc())
)
