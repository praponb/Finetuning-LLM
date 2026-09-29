from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNull() & (F.col("sale_date") < "2023-11-27") & (F.col("units") != 25))
    .groupBy(
        "store_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") >= 5)
    .drop("revenue_count")
)
