from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-09-18").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.length("status").alias("status_length"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "carrier",
        F.col("cost").alias("COST"),
        F.col("cost_filled").alias("COST_FILLED"),
    )
    .unpivot(["id", "carrier"], ["COST", "COST_FILLED"], "metric_name", "val")
    .filter(F.col("val").isNotNull())
)
