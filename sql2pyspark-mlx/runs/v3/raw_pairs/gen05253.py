from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 79)
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
    )
    .filter(F.col("returned_cost") > 22)
)
