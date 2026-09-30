from pyspark.sql import functions as F

result = (
    df_accounts
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "SAVINGS"]))
        .select("id")
    )
)
