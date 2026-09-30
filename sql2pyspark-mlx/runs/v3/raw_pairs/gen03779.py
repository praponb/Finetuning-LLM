from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date").between("2024-03-01", "2025-10-14") & F.col("customer_id").isin([8, 12, 23, 32])) | F.col("account_type").isin(["SAVINGS", "CREDIT"]))
    .groupBy(
        F.when(F.col("balance") >= 80000, "high").when(F.col("balance") >= 31000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
    )
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .orderBy(F.col("avg_balance").desc())
)
