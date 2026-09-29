from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("YEAR", "delivered_date").alias("year_start"))
    .agg(
        F.max("cost").alias("max_cost"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
    )
    .orderBy("in_transit_rows")
)
