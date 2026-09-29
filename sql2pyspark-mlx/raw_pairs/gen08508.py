from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 27) & (F.col("units") <= 12))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "product_id",
    )
    .agg(
        F.count("units").alias("units_count"),
        F.min("sale_date").alias("first_sale_date"),
    )
    .orderBy(F.col("units_count").desc())
)
