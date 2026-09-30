from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "customer_id", F.month("opened_date").alias("opened_month"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .orderBy(F.col("status").desc())
)
