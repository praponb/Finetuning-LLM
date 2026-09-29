from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 12) | (F.col("store_id") == 1))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy(F.col("cnt").desc())
)
