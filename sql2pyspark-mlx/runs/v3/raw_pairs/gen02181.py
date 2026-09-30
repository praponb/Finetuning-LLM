from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 110)
    .select("end_date")
    .subtract(df_shipments.select("delivered_date"))
)
