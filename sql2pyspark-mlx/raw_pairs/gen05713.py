from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.avg("units").alias("avg_units"),
    )
    .filter(F.col("avg_revenue") < 3000)
    .select("sale_cohort", "avg_revenue", "avg_units")
)
