from pyspark.sql import functions as F

base = df_shipments.filter(F.col("carrier") == "Flash")

status_summary = (
    base
    .groupBy("status")
    .agg(
        F.min("shipped_date").alias("first_shipped_date"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    status_summary
    .select("status", "first_shipped_date", "total_cost")
    .orderBy(F.col("total_cost").desc(), "status")
    .limit(3)
)
