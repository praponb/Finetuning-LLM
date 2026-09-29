from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") < 29) & F.month("sale_date").isin([1, 10, 11]))
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)
