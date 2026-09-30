from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 4, 7]))
    .select("plan")
    .distinct()
    .orderBy("plan")
)
