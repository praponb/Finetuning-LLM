from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.sum("balance").alias("total_balance"),
    )
    .orderBy(F.col("total_balance").desc())
)
