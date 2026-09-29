from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2023)
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.avg("units").alias("avg_units"),
    )
    .orderBy(F.col("stddev_revenue").desc())
)
