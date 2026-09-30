from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.when(F.col("balance") >= 73000, "high").when(F.col("balance") >= 26000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.avg("balance").alias("avg_balance"),
    )
    .filter(F.col("avg_balance") < 26000)
)
