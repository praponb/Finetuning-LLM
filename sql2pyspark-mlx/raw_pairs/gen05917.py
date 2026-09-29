from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.year("sale_date").alias("sale_year"))
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("stddev_units") > 61)
)
