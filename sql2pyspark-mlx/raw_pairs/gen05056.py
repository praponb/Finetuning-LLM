from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNotNull())
    .select(F.col("cost").alias("value"), F.lit("shipment").alias("record_type"))
    .unionByName(
        df_products
        .filter(F.col("price").isNotNull())
        .select(F.col("price").alias("value"), F.lit("product").alias("record_type"))
    )
)
