from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy(F.col("avg_units").desc())
)
