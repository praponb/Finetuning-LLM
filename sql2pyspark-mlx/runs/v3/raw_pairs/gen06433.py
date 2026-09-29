from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "enterprise") | (F.col("status") == "ACTIVE"))
    .select("start_date", "plan", "monthly_fee")
)
