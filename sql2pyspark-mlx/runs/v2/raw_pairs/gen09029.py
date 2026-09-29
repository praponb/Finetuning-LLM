from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2024-08-01")
    .groupBy(
        F.when(F.col("units") >= 43, "high").when(F.col("units") >= 13, "mid").otherwise("low").alias("units_bucket"),
        "store_id",
    )
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
    .orderBy(F.col("units_bucket").desc(), "store_id")
    .limit(10)
)
