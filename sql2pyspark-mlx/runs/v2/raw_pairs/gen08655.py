from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "Flash"]))
    .groupBy("status")
    .agg(
        F.count("*").alias("num_shipments"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") < 90)
    .orderBy(F.col("status").desc())
)
