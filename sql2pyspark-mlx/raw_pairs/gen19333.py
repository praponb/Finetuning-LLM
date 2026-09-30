from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type")
    .intersectAll(
        df_accounts
        .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]))
        .select("account_type")
    )
)
