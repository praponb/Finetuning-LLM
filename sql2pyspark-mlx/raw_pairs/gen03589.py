from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 39, "LARGE").otherwise("SMALL").alias("units_tier"),
        "product_id",
    )
    .agg(
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.count("*").alias("cnt"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.percentile_approx("units", 0.95).alias("units_p95_2"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("cnt")
)
