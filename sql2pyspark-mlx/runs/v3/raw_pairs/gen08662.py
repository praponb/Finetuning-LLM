from pyspark.sql import functions as F

src = (
    df_shipments
    .filter((F.year("delivered_date") >= 2025) & (F.col("shipped_date") >= "2024-10-01") & (F.col("status") == "RETURNED"))
)

summary = (
    src
    .groupBy(
        F.year("delivered_date").alias("delivered_year"),
        F.month("delivered_date").alias("delivered_month"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    summary
    .select("delivered_year", "delivered_month", "avg_cost", "total_cost")
    .orderBy(F.col("delivered_year").desc())
)
