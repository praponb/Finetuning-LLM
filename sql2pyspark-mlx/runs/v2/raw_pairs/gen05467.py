from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.count("*").alias("num_shipments"),
        F.avg("cost").alias("avg_cost"),
    )
)
