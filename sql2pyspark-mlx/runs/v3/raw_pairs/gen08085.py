from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(50000, 69000))
    .groupBy(
        F.when(F.col("balance") >= 67000, "high").when(F.col("balance") >= 33000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.count("balance").alias("balance_count"),
    )
    .orderBy(F.col("balance_count").desc(), "balance_bucket")
    .limit(50)
)
