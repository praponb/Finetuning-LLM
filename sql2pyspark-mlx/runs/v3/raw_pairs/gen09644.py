from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.month("start_date").isin([3, 6, 12]))
        .select("status")
    )
    .orderBy("status")
)
