from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.min("shipped_date").alias("min_shipped_date"),
    )
    .orderBy("carrier", F.col("returned_cost").desc())
)
