from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(
        df_accounts
        .filter(~F.col("status").isin(["OPEN", "CLOSED"]))
        .select("opened_date")
    )
)
