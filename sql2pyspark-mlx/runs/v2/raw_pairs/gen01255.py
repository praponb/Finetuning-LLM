from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
    )
)
