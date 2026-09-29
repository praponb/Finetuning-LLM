from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
