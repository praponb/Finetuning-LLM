from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.max("cost").alias("max_cost"),
        F.count("*").alias("shipment_count"),
    )
    .filter(F.col("max_cost") < 82)
    .orderBy(F.col("max_cost").desc())
)
