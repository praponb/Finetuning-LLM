from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2023-12-01")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.min("delivered_date").alias("min_delivered_date"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)
