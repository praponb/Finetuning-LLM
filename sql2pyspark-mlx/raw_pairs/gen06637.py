from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(54, 65))
    .groupBy("carrier", "status")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
    .orderBy(F.col("avg_cost").desc(), "carrier", "status")
    .limit(10)
)
