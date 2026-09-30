from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2023)
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 4)
    .orderBy("returned_cost")
)
