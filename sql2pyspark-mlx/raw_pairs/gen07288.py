from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "EXPIRED"]))
    .select("start_date")
    .intersect(df_accounts.select(F.col("opened_date").alias("start_date")))
)
