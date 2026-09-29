from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.count("*").alias("total_count"),
    )
)
