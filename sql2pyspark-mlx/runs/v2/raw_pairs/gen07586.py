from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("delivered_date") <= "2025-07-02") | F.col("order_id").isNull())
)

status_summary = (
    base
    .groupBy("status", F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    status_summary
    .filter(F.col("avg_cost") > 30)
    .orderBy(F.col("avg_cost").desc(), "status", "shipped_month")
    .limit(1)
)
