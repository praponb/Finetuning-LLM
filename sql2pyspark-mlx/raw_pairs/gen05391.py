from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.lit("customer").alias("record_type"), "signup_date")
    .unionByName(
        df_shipments
        .select(
        F.lit("shipment").alias("record_type"),
        F.col("delivered_date").alias("signup_date"),
    )
    )
    .distinct()
    .orderBy("signup_date")
)
