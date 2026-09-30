from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.avg("units").alias("avg_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)
