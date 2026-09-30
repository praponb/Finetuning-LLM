from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([5, 8, 27]))
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
    )
    .agg(
        F.min("price").alias("min_price"),
        F.count("*").alias("product_count"),
        F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
    )
    .filter(F.col("product_count") < 3)
    .orderBy("launch_month", F.col("min_price").desc(), "launch_cohort")
    .limit(5)
)
