from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.when(F.col("cost") >= 88, "High").when(F.col("cost") >= 15, "Medium").otherwise("Low").alias("cost_band"),
    )
)
