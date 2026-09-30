from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        (F.datediff(F.current_date(), "shipped_date") / 365).alias("years_since_shipped_date"),
        F.lower("status").alias("status_lower"),
    )
)
