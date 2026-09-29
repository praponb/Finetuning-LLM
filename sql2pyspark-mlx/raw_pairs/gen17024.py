from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.date_trunc("year", "sale_date").alias("year_start"))
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("revenue").alias("total_revenue_2"),
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
)
