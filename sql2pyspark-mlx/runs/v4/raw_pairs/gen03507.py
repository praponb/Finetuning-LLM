from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & (F.col("monthly_fee") >= 170))
    .select("id", "start_date")
    .orderBy("start_date")
)
