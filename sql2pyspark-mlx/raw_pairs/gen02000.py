from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.countDistinct("cost").alias("unique_carriers"),
        F.max("shipped_date").alias("last_shipped_date"),
    )
)
