from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([3, 7, 8]))
    .select("plan")
    .distinct()
    .orderBy("plan")
    .limit(20)
)
