from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 24000)
    .select("customer_id", "opened_date")
    .intersect(
        df_accounts
        .filter(F.col("account_type") == "CREDIT")
        .select("customer_id", "opened_date")
    )
    .orderBy(F.col("customer_id").desc())
)
