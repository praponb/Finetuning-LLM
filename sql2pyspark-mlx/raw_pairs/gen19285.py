from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.lit("shipment").alias("source"), F.col("carrier").alias("label"))
    .unionByName(
        df_web_events
        .select(F.lit("web_event").alias("source"), F.col("page").alias("label"))
    )
    .distinct()
)
