from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date", F.lit("product").alias("source"))
    .union(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("delivered_date", F.lit("shipment").alias("source"))
    )
)
