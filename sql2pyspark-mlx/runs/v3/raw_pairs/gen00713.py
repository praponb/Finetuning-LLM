from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .union(
        df_subscriptions
        .filter(F.col("status") == "CANCELLED")
        .select("end_date")
    )
    .distinct()
)
