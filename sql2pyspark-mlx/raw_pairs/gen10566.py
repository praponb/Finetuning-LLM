from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").isNotNull())
    .select("delivered_date")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date").isNotNull())
        .select(F.col("end_date").alias("delivered_date"))
    )
    .orderBy(F.col("delivered_date").desc())
)
