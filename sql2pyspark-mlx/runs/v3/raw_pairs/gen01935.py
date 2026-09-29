from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date", F.lit("shipment").alias("source"))
    .unionByName(
        df_products
        .filter(F.col("supplier_id").isin([4, 5, 18, 37]))
        .select(
        F.col("launch_date").alias("delivered_date"),
        F.lit("product").alias("source"),
    )
    )
    .distinct()
)
