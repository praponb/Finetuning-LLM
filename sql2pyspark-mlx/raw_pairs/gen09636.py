from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 59000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
    )
)
