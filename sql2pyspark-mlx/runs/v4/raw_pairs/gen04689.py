from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .select("customer_id")
    .unionByName(
        df_accounts
        .filter(~F.col("account_type").isin(["CREDIT", "LOAN"]))
        .select("customer_id")
    )
    .distinct()
)
