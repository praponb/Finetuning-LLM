from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["Kerry", "DHL"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"))
    .orderBy(F.col("delivered_count").desc())
)
