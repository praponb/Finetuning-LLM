from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.month("end_date").isin([1, 2, 6]))
        .select("status")
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
