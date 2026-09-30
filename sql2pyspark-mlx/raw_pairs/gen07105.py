from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([6, 8, 9]))
    .groupBy("status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
    .orderBy(F.col("cost_count").desc())
)
