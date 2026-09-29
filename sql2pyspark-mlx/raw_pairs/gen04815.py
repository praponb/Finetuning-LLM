from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("order_id").isin([19, 20, 33]) & (F.col("carrier") == "DHL")) | F.month("shipped_date").isin([1, 5, 11]))
)

agg = (
    valid_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count("*").alias("row_count"),
    )
)

result = (
    agg
    .select("delivered_month", "avg_cost", "row_count")
    .orderBy(F.col("delivered_month").desc())
    .limit(3)
)
