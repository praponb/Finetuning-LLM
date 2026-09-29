from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", "status")
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
    .filter(F.col("pct_delivered") < 46)
    .orderBy("carrier")
)
