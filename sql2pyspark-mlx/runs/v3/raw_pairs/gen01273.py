from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2025-03-23")
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "SAVINGS", "LOAN"]))
        .select("status")
    )
    .orderBy("status")
)
