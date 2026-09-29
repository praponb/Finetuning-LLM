from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 2100) | F.col("revenue").between(1200, 2000))
    .groupBy(
        "product_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("revenue").alias("min_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
    .filter(F.col("max_revenue") < 3500)
    .drop("max_revenue")
)
