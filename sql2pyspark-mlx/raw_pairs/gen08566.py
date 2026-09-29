from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isin([19, 23, 24]))
        .select("start_date")
    )
)
