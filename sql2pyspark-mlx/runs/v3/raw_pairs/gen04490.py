from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("id", "status", "customer_id")
    .unionByName(
        df_accounts
        .filter(~F.col("account_type").isin(["LOAN", "CREDIT", "SAVINGS"]))
        .select("id", "status", "customer_id")
    )
)
