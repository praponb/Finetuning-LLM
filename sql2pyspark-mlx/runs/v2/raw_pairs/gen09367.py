from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 79)
    .select(
        "delivered_date",
        "carrier",
        "status",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
    .orderBy(F.col("days_since_shipped_date").desc())
)
