from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 42, "high").when(F.col("units") >= 17, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .filter(F.col("units_range") < 40)
    .orderBy("units_bucket")
    .limit(10)
)
