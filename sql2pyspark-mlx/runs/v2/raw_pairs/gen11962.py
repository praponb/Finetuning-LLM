from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.min("sale_date").alias("min_sale_date"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .filter(F.col("stddev_units") > 36)
)
