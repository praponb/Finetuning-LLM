from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.lit("shipment").alias("record_type"), "shipped_date")
    .union(
        df_customers
        .filter(F.col("segment").isin(["Corporate", "SMB"]))
        .select(F.lit("customer").alias("record_type"), "signup_date")
    )
    .distinct()
)
