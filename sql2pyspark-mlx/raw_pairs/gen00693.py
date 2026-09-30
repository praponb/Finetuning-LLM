from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-07-01", "2025-03-01") & (F.col("delivered_date") > "2023-10-01"))
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.avg("cost").alias("avg_cost"),
    )
    .orderBy("shipped_month", F.col("avg_cost").desc())
    .limit(5)
)
