from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%rac%"))
    .select(F.col("signup_date").alias("event_date"), F.lit("customer").alias("source"))
    .unionByName(
        df_shipments
        .select(
        F.col("delivered_date").alias("event_date"),
        F.lit("shipment").alias("source"),
    )
    )
    .distinct()
)
