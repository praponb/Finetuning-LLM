from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.lit("store").alias("record_type"), "store_name")
    .union(df_shipments.select(F.lit("shipment").alias("record_type"), "status"))
    .distinct()
)
