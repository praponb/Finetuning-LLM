from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.lit("shipment").alias("record_type"), F.col("cost").alias("value"))
    .unionByName(
        df_store_sales
        .filter(F.col("store_id") == 1)
        .select(F.lit("store_sale").alias("record_type"), F.col("revenue").alias("value"))
    )
)
