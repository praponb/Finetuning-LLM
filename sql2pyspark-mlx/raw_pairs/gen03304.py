from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select("signup_date", F.lit("customer").alias("kind"))
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date").isNotNull())
        .select(F.col("shipped_date").alias("signup_date"), F.lit("shipment").alias("kind"))
    )
    .distinct()
    .orderBy("signup_date")
)
