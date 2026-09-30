from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("ups_count") >= 23)
    .select("carrier", "ups_count", "cost_count")
    .orderBy(F.col("ups_count").desc(), "carrier")
    .limit(5)
)
