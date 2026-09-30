from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

latest = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("week", "sale_date").alias("week_start"), "product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_or("units").alias("units_bit_or"),
    )
    .orderBy(F.col("units_bit_or").desc())
)
