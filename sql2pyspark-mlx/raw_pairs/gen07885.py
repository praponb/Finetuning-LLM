from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.lit("order_item").alias("record_type"), "unit_price")
    .union(df_shipments.select(F.lit("shipment").alias("record_type"), "cost"))
    .distinct()
    .orderBy("record_type")
)
