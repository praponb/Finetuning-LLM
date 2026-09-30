from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-04-01", "2024-01-01") | (F.col("status") == "DELIVERED"))
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy(F.col("min_cost").desc())
)
