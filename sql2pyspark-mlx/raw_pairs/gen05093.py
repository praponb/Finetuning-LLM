from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id", "status")
    .intersect(
        df_subscriptions
        .filter(F.month("end_date").isin([1, 8, 9]))
        .select("customer_id", "status")
    )
)
