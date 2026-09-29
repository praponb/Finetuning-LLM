from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
    .filter(F.col("pct_delivered") < 30)
    .select("status", "ups_count", "pct_delivered")
    .orderBy(F.col("ups_count").desc(), F.col("pct_delivered").desc(), "status")
    .limit(50)
)
