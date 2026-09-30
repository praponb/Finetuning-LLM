from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("order_id").isNotNull() & (F.col("status") == "IN_TRANSIT") & (F.col("cost") >= 71)))
    .groupBy("order_id", F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 5)
    .orderBy(F.col("avg_cost").desc())
)
