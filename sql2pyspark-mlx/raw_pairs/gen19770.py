from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 2800) & (F.col("store_id") == 5))
    .groupBy(
        "product_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.min("revenue").alias("min_revenue"))
    .filter(F.col("min_revenue") > 2100)
    .orderBy(F.col("min_revenue").desc())
)
