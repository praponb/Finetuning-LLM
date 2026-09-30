from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.month("end_date").isin([4, 5, 9]))
        .select("status")
    )
    .distinct()
    .orderBy("status")
)
