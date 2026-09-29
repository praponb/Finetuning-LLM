from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 170)
    .select("end_date")
    .union(df_shipments.select("delivered_date"))
    .orderBy("end_date")
)
