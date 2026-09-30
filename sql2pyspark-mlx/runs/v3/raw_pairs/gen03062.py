from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select("shipped_date")
    .intersect(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "PAUSED", "CANCELLED"]))
        .select("end_date")
    )
)
