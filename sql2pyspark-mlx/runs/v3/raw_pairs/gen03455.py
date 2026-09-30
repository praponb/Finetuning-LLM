from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("account_type") == "LOAN"))
    .groupBy(
        F.when(F.col("account_type").isin(["LOAN", "CREDIT"]), "core").otherwise("other").alias("account_type_group"),
    )
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
    )
    .filter(F.col("credit_balance") >= 37)
    .orderBy(F.col("credit_balance").desc())
)
