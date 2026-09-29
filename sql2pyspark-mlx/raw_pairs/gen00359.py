from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("order_id").isNull() & F.col("cost").between(54, 90) & (F.col("status") == "DELIVERED"))
)

shipments_stats = (
    filtered_shipments
    .groupBy(F.year("delivered_date").alias("delivered_year"), "status")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    shipments_stats
    .filter(F.col("cost_range") < 61)
    .select("delivered_year", "status", "last_shipped_date")
)
