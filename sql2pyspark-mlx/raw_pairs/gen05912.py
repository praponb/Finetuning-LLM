from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 49000)
    .groupBy(
        F.when(F.col("balance") >= 78000, "high").when(F.col("balance") >= 35000, "mid").otherwise("low").alias("balance_bucket"),
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "group_a").otherwise("other").alias("status_group"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
        F.max("opened_date").alias("latest_opened_date"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
    .orderBy("latest_opened_date", "balance_bucket", "status_group", "status")
    .limit(1)
)
