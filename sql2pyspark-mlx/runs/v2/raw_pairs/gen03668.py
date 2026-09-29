from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "OPEN"]) & (F.col("status") == "CLOSED"))
    .withColumn("balance_scaled", F.round((F.col("balance") * 0.85), 2))
)

accounts_stats = (
    valid_accounts
    .groupBy("status", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.round(F.avg("balance_scaled"), 2).alias("avg_balance_scaled"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)

result = (
    accounts_stats
    .filter(F.col("avg_balance_scaled") < 55)
    .select("status", "opened_quarter", "avg_balance_scaled", "account_type_list")
    .orderBy(F.col("avg_balance_scaled").desc(), "status", "opened_quarter")
    .limit(20)
)
