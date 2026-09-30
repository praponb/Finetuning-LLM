from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan").isin(["enterprise", "basic", "standard"]))
        .select("status")
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
