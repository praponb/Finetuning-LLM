from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED", "ACTIVE"]))
    .select("start_date")
    .intersect(
        df_accounts
        .filter(F.col("balance").between(62000, 67000))
        .select(F.col("opened_date").alias("start_date"))
    )
    .orderBy(F.col("start_date").desc())
)
