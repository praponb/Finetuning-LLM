from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.bit_and("units").alias("units_bit_and"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.min("revenue").alias("min_revenue"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
)
