from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id").isin([1, 2, 4, 5]) & (F.col("units") == 11)) | (F.col("sale_date") >= "2023-07-01"))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        F.when(F.col("units") >= 37, "high").when(F.col("units") >= 14, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("*").alias("num_store_sales"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("min_units") >= 18)
    .drop("min_units")
    .orderBy("sale_cohort")
)
