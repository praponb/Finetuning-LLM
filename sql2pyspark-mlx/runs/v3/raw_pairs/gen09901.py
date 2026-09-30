from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select("opened_date")
    .intersect(
        df_accounts
        .filter(F.col("account_type").isin(["SAVINGS", "CREDIT"]))
        .select("opened_date")
    )
    .orderBy("opened_date")
)
