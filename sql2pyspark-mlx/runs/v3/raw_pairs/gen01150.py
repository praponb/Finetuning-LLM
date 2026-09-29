from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-08-07", "2025-09-12") & (F.col("delivered_date") < "2023-05-15"))
    .withColumn("shipped_date_period", F.date_trunc("YEAR", "shipped_date"))
)

status_summary = (
    base
    .groupBy("status")
    .agg(
        F.max("cost").alias("max_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    status_summary
    .select("status", "max_cost")
    .orderBy("max_cost")
)
