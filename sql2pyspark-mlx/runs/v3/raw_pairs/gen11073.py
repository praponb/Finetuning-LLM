from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([18, 24, 31]) & (F.col("customer_id") == 5) & F.col("balance").between(38000, 55000))
    .groupBy(
        F.when(F.col("account_type").isin(["CREDIT", "LOAN"]), "tier_1").otherwise("other").alias("account_type_group"),
    )
    .agg(
        F.countDistinct("balance").alias("unique_account_types"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
    )
)
