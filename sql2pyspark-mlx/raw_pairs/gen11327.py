from pyspark.sql import functions as F

result = (
    df_shipments
    .rollup(
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
        F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .filter(F.col("in_transit_rows") > 1)
    .orderBy("in_transit_rows")
)
