from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 65000, "high").when(F.col("balance") >= 35000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
    )
    .orderBy("balance_bucket")
)
