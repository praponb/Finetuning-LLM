from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id", F.month("opened_date").alias("opened_month"))
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
    )
    .orderBy(F.col("checking_count").desc(), F.col("opened_month").desc())
)
