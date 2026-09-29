from pyspark.sql import functions as F

src = df_store_sales.filter((F.col("store_id") == 6) & (F.col("units") > 24))

summary = (
    src
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    summary
    .filter(F.col("avg_revenue") < 1200)
    .orderBy("sale_cohort")
)
