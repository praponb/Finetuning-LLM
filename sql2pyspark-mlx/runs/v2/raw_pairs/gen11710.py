from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 15) & F.col("shipped_date").between("2023-11-25", "2024-05-01"))
    .groupBy("status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") >= 90)
)
