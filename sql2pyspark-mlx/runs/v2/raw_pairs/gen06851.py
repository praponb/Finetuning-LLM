from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("start_date") == 2024) & (F.col("start_date") > "2024-11-01") & ~F.col("status").isin(["EXPIRED", "PAUSED", "CANCELLED"]))
    .select("end_date", "start_date")
)
