from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.count("*").alias("num_store_sales"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy(F.col("num_store_sales").desc())
)
