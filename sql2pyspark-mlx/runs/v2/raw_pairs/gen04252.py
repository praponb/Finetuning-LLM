from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 2, 8]))
    .select("plan", "status")
    .distinct()
    .orderBy("plan")
)
