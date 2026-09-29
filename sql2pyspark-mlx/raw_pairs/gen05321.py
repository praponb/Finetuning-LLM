from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("revenue") > 1500)

store_sales_stats = (
    base
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("units").alias("units_count"),
    )
)

result = (
    store_sales_stats
    .select("sale_cohort", "avg_revenue", "units_count")
    .orderBy(F.col("units_count").desc())
)
