from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("cost") >= 80) & F.col("carrier").isNotNull() & F.col("order_id").isin([3, 9, 20]))
)

summary = (
    recent_shipments
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    summary
    .filter(F.col("min_cost") >= 55)
    .orderBy("last_shipped_date", "shipped_month")
    .limit(10)
)
