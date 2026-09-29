from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date", F.lit("shipment").alias("source"))
    .union(df_stores.select("opened_date", F.lit("store").alias("source")))
)
