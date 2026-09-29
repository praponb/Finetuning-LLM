from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
    )
)
