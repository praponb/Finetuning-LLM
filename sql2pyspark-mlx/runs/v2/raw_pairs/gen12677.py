from pyspark.sql import functions as F

result = (
    df_shipments
    .cube(F.quarter("delivered_date").alias("delivered_quarter"), "status")
    .agg(
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
        F.min("shipped_date").alias("min_shipped_date"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .orderBy(F.col("delivered_quarter").desc())
)
