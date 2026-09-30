from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.month("end_date").isin([2, 5, 12]))
        .select("status")
    )
    .distinct()
    .orderBy("status")
)
