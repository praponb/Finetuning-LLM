from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 11)
    .rollup(
        "store_id",
        F.when(F.col("units") >= 31, "high").when(F.col("units") >= 20, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.min("units").alias("min_units"))
    .orderBy("units_bucket")
)
