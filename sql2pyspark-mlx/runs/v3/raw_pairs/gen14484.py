from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.avg("units").alias("avg_units"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
    .drop("cnt")
)
