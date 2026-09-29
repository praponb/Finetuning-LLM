from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
    )
    .filter(F.col("delivered_cost") > 49)
    .drop("delivered_cost")
)
