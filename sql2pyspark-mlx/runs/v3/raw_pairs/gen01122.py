from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("balance") <= 76000) & (~F.col("account_type").isin(["CHECKING", "CREDIT", "LOAN"]) | F.col("opened_date").between("2025-03-30", "2025-09-01"))))
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "customer_id",
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
    )
)
