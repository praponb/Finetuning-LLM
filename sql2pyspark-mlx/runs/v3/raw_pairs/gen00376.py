from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.count("cost").alias("cost_count"),
        F.count("*").alias("total_count"),
    )
)
