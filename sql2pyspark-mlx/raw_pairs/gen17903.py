from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", "status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy("carrier", "status")
)
