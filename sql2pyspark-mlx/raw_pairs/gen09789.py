from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 34)
    .agg(
        F.max("units").alias("max_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy(F.col("units_range").desc())
)
