from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .union(
        df_accounts
        .filter(~F.col("status").isin(["OPEN", "CLOSED"]))
        .select("opened_date")
    )
    .orderBy("end_date")
)
