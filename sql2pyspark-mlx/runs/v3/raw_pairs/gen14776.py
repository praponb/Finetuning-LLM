from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([17, 28, 33]) & (F.col("shipped_date") <= "2023-09-01") & (F.col("shipped_date") < "2025-08-14"))
)

shipments_stats = (
    filtered_shipments
    .groupBy(
        F.year("delivered_date").alias("delivered_year"),
        F.year("shipped_date").alias("shipped_year"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    shipments_stats
    .filter(F.col("avg_cost") < 44)
    .orderBy(F.col("delivered_year").desc())
)
