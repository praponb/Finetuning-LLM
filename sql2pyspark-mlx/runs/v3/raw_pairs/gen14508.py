from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .groupBy("carrier")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
    )
    .filter(F.col("avg_cost") > 58)
    .orderBy("pct_kerry")
)
