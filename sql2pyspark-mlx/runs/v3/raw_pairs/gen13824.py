from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 42, "high").when(F.col("units") >= 15, "mid").otherwise("low").alias("units_bucket"),
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") < 1)
    .orderBy("unique_stores")
)
