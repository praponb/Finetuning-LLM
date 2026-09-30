from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 4, 11]))
    .groupBy("plan")
    .pivot("status", ["EXPIRED", "ACTIVE"])
    .agg(F.sum("monthly_fee"))
)
