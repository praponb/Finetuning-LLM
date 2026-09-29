from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(57000, 73000))
    .select(F.col("opened_date").alias("event_date"))
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "standard")
        .select(F.col("end_date").alias("event_date"))
    )
    .distinct()
)
