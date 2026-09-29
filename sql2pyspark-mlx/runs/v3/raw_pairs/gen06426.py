from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 24) & F.col("product_id").isin([2, 7, 8, 10]) & (F.col("units") < 28))
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)
