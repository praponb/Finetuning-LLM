from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date").between("2023-07-01", "2023-09-01"))
        .select("status")
    )
)
