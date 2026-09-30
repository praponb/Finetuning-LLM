from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "order_id",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)
