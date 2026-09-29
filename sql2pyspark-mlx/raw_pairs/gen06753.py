from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan")
    .intersect(
        df_subscriptions
        .filter(F.month("end_date").isin([1, 3, 8]))
        .select("plan")
    )
)
