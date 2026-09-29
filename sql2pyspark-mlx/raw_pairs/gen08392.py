from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select(F.lit("customer").alias("source"), F.col("signup_date").alias("event_date"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date").isNotNull())
        .select(
        F.lit("shipment").alias("source"),
        F.col("delivered_date").alias("event_date"),
    )
    )
    .distinct()
    .orderBy(F.col("event_date").desc())
)
