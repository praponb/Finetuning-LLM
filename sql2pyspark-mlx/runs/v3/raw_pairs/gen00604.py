from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .subtract(
        df_subscriptions
        .filter(F.col("status") == "ACTIVE")
        .select("end_date")
    )
    .orderBy("opened_date")
)
