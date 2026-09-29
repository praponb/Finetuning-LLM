from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date", F.lit("shipment").alias("source"))
    .union(
        df_products
        .filter(F.size("tags") >= 2)
        .select("launch_date", F.lit("product").alias("source"))
    )
)
