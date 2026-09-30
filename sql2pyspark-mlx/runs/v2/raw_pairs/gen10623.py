from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "PAUSED"]))
    .select("id", "plan", F.add_months("start_date", 3).alias("start_date_plus_months"))
)
