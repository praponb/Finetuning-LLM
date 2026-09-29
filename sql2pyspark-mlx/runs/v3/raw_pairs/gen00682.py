from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 20) & (F.col("opened_date") < "2023-11-01"))
    .groupBy(
        F.when(F.col("balance") >= 84000, "high").when(F.col("balance") >= 28000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("checking_balance") < 51)
)
