from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([6, 10, 11]))
    .select("id", "account_type", "opened_date")
    .intersect(
        df_accounts
        .filter(~F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]))
        .select("id", "account_type", "opened_date")
    )
)
