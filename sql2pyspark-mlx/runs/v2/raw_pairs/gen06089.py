from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("customer_id") == 6) & (F.col("account_type") == "CHECKING"))
)

agg = (
    recent_accounts
    .groupBy(
        F.when(F.col("balance") >= 87000, "high").when(F.col("balance") >= 16000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    agg
    .filter(F.col("max_balance") >= 55000)
    .select("balance_bucket", "account_type", "max_balance", "status_values")
    .orderBy(F.col("account_type").desc(), F.col("max_balance").desc())
)
