from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 40, "high").when(F.col("units") >= 11, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.min("units").alias("min_units"))
    .filter(F.col("min_units") > 16)
    .orderBy(F.col("units_bucket").desc(), "min_units")
)
