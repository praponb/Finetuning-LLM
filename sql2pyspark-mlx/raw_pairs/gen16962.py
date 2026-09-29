from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(~F.col("status").isin(["IN_TRANSIT", "DELIVERED"]))
    .select(
        "id",
        "shipped_date",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("days_since_delivered_date") >= 87, "HIGH").otherwise("LOW").alias("days_since_delivered_date_tier"),
    )
    .agg(
        F.percentile_approx("days_since_delivered_date", [0.25, 0.5, 0.75]).alias("days_since_delivered_date_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
