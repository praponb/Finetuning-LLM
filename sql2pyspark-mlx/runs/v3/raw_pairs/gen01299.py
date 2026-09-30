from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 1300) | F.col("store_id").isin([2, 6]))
    .groupBy("product_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy("product_id")
)
