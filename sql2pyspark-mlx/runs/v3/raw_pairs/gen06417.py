from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2025-03-08", "2025-06-01"))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
    )
    .filter(F.col("credit_balance") < 33)
    .select("customer_id", "credit_balance")
    .orderBy(F.col("customer_id").desc())
)
