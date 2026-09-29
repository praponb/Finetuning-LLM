from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([12, 14, 27, 35]))
    .select(F.col("delivered_date").alias("event_date"))
    .intersect(
        df_customers
        .filter(~F.col("email").like("%@startup.io"))
        .select(F.col("signup_date").alias("event_date"))
    )
    .orderBy(F.col("event_date").desc())
)
