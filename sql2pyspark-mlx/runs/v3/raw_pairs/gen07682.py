from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 67000, "high").when(F.col("balance") >= 32000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
    .filter(F.col("pct_savings") < 44)
)
