from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.year("sale_date") == 2025)

sale_cohort_summary = (
    filtered_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = sale_cohort_summary
