from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-03-01", "2025-07-18"))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.count("*").alias("num_store_sales"))
)
