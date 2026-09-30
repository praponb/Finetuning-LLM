from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("shipped_date").alias("event_date"))
    .intersect(
        df_stores
        .filter(F.col("city").isin(["Singapore", "London"]))
        .select(F.col("opened_date").alias("event_date"))
    )
    .orderBy("event_date")
)
