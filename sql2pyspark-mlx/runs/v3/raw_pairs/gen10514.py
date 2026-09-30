from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(80, 96))
    .groupBy(
        "status",
        "carrier",
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("delivered_date").alias("last_delivered_date"),
    )
    .filter(F.col("avg_cost") > 41)
    .orderBy("shipped_month")
)
