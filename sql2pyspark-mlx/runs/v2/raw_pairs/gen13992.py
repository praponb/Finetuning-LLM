from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", "status")
    .agg(F.countDistinct("cost").alias("unique_carriers"))
    .orderBy("carrier", F.col("unique_carriers").desc())
)
