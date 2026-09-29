from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CREDIT"]) & F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]) & F.col("customer_id").isNotNull())
    .groupBy(F.year("opened_date").alias("opened_year"), "account_type")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
    )
    .orderBy(F.col("opened_year").desc(), "closed_balance")
)
