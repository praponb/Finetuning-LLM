from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("U%"))
    .select(F.lit("product").alias("record_type"), "launch_date")
    .union(
        df_shipments
        .filter(F.col("order_id").isin([17, 23, 33, 39]))
        .select(F.lit("shipment").alias("record_type"), "delivered_date")
    )
    .distinct()
)
