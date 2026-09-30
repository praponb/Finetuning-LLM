from pyspark.sql import functions as F

result = (
    df_accounts
    .groupingSets(
        [["customer_id", "account_type"], ["customer_id"]],
        "customer_id",
        "account_type",
    )
    .agg(
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("loan_balance") > 0)
    .orderBy("customer_id")
)
