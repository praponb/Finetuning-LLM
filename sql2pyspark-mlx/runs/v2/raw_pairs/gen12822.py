from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("ups_count") > 1)
    .orderBy(F.col("ups_count").desc())
)
