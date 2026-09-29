from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 41, "high").when(F.col("units") >= 20, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("*").alias("num_store_sales"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("units_range") < 22)
    .select("units_bucket", "num_store_sales", "units_range")
)
