from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").isNotNull())
    .select("end_date")
    .intersect(
        df_shipments
        .filter(F.col("delivered_date").isNotNull())
        .select("delivered_date")
    )
    .orderBy(F.col("end_date").desc())
)
