from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").isNotNull())
    .select(F.lit("shipment").alias("record_type"), "delivered_date")
    .union(
        df_stores
        .filter(F.col("opened_date").isNotNull())
        .select(F.lit("store").alias("record_type"), "opened_date")
    )
    .orderBy("record_type")
)
