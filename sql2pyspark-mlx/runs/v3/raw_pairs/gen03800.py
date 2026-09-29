from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") <= "2023-08-01") & (F.col("monthly_fee") > 70))
    .select("id", "start_date")
    .orderBy(F.col("start_date").desc(), "id")
    .limit(3)
)
