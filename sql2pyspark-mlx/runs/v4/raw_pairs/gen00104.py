from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.min("revenue").alias("min_revenue"),
        F.countDistinct("units").alias("unique_products"),
    )
    .filter(F.col("total_count") > 5)
)
