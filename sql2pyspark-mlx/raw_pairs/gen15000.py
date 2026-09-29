from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").isNotNull())
    .select("launch_date", F.lit("product").alias("record_type"))
    .union(
        df_shipments
        .filter(F.col("delivered_date").isNotNull())
        .select("delivered_date", F.lit("shipment").alias("record_type"))
    )
    .distinct()
)
