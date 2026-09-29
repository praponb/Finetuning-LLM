from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2023-07-24")
    .groupBy(
        F.when(F.col("balance") >= 83000, "high").when(F.col("balance") >= 25000, "mid").otherwise("low").alias("balance_bucket"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count("*").alias("total_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy("balance_bucket")
)
