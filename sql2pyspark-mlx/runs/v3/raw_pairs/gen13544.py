from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2024)
    .select("plan")
    .subtract(df_subscriptions.filter(F.col("monthly_fee") > 160).select("plan"))
)
