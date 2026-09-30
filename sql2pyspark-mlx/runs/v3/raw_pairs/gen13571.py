from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("cost_count") >= 3)
    .drop("cost_count")
)
