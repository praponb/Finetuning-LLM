from pyspark.sql import functions as F

src = df_shipments.filter(F.year("delivered_date") == 2024)

status_summary = (
    src
    .groupBy("status")
    .agg(
        F.max("cost").alias("max_cost"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    status_summary
    .filter(F.col("avg_cost") >= 67)
    .orderBy("max_cost", "status")
    .limit(5)
)
