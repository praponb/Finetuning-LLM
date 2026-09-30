from pyspark.sql import functions as F

result = (
    df_products
    .select(F.lit("product").alias("record_type"), "price")
    .union(df_shipments.select(F.lit("shipment").alias("record_type"), "cost"))
)
