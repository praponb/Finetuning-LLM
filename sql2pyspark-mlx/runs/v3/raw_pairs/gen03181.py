from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .union(
        df_accounts
        .filter(~F.col("status").isin(["FROZEN", "OPEN"]))
        .select("opened_date")
    )
    .distinct()
    .orderBy("start_date")
)
