from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").isNotNull())
    .select("shipped_date", F.lit("shipment").alias("record_type"))
    .union(
        df_stores
        .filter(F.col("opened_date").isNotNull())
        .select("opened_date", F.lit("store").alias("record_type"))
    )
    .orderBy("record_type")
)
