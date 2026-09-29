from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(((F.col("shipped_date") >= "2024-09-01") & F.col("carrier").isin(["DHL", "Flash", "Kerry"])) | (F.col("delivered_date") <= "2025-01-01"))
)

delivered_month_summary = (
    recent_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.sum("cost").alias("total_cost"),
    )
)

result = delivered_month_summary
