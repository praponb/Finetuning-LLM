from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") <= "2025-05-01") & (F.col("order_id") == 22))
    .agg(
        F.max("delivered_date").alias("last_delivered_date"),
        F.max("cost").alias("max_cost"),
        F.count("cost").alias("cost_count"),
    )
)
