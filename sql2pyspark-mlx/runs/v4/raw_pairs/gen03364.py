from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") >= "2024-08-01") | F.col("customer_id").isNotNull())
    .groupBy("status")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy("first_opened_date")
)
