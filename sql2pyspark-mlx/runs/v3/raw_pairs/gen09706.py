from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.countDistinct("cost").alias("unique_statuss"),
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
    .filter(F.col("returned_count") < 5)
    .orderBy("returned_count", "shipped_month")
    .limit(5)
)
