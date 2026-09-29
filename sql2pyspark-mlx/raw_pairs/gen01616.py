from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-09-01")
    .select(
        "id",
        "status",
        "cost",
        "shipped_date",
        F.round((F.datediff("shipped_date", "delivered_date") / 30), 1).alias("months_delivered_date_to_shipped_date"),
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
    )
    .orderBy(F.col("days_since_delivered_date").desc())
)
