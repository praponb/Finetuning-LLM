from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(F.avg("revenue").alias("avg_revenue"))
    .orderBy(F.col("avg_revenue").desc(), "sale_cohort", "sale_year")
    .limit(50)
)
