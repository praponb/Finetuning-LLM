from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.count("*").alias("shipment_count"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy(F.col("flash_cost").desc())
)
