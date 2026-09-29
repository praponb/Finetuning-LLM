from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.countDistinct("balance").alias("unique_account_types"),
    )
    .filter(F.col("avg_balance") >= 54000)
    .orderBy(F.col("unique_account_types").desc(), "customer_id")
    .limit(5)
)
