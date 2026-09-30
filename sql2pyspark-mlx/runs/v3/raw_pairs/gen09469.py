from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 38, "high").when(F.col("units") >= 20, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.min("units").alias("min_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("min_units") > 21)
)
