from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("min_units") > 31)
)
