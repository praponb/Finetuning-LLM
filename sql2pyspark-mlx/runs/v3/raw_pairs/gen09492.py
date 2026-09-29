from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([2, 7, 8]))
    .select("delivered_date")
    .intersect(
        df_subscriptions
        .filter(F.col("plan") == "premium")
        .select("start_date")
    )
)
