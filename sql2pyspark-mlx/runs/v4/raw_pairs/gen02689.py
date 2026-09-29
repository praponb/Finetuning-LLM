from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
)
