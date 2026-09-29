from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").isNotNull())
    .select("shipped_date")
    .union(
        df_subscriptions
        .filter(F.col("end_date").isNotNull())
        .select("end_date")
    )
)
