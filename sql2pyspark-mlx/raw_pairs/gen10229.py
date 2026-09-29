from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-08-23").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
    )
)

result = (
    cleaned
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "IN_TRANSIT"])
    .agg(
        F.sum("days_since_shipped_date").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
