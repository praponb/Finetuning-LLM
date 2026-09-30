from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select("start_date")
    .union(df_stores.select("opened_date"))
    .distinct()
)
