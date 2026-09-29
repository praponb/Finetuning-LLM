from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select("status", "opened_date")
    .unionByName(
        df_accounts
        .filter(~F.col("account_type").isin(["LOAN", "CHECKING"]))
        .select("status", "opened_date")
    )
    .distinct()
)
