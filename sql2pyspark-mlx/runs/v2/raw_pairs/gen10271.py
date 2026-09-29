from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count("*").alias("row_count"),
        F.avg("cost").alias("avg_cost"),
    )
    .orderBy("avg_cost", "carrier")
    .limit(5)
)
