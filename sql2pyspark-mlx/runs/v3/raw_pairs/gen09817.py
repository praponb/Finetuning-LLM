from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 3)
    .drop("total_count")
    .orderBy(F.col("total_revenue").desc(), "sale_month")
)
