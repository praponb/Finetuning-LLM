from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.countDistinct("revenue").alias("unique_stores"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy("sale_month")
)
