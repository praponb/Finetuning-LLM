from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNull())
    .select("delivered_date", F.lit("shipment").alias("source"))
    .union(
        df_stores
        .filter(F.col("region") == "North")
        .select("opened_date", F.lit("store").alias("source"))
    )
)
