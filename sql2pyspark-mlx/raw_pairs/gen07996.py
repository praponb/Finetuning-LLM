from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date").between("2024-06-24", "2024-09-30") & F.col("product_name").like("D%")) | (F.size("tags") >= 1))
    .groupBy(
        "category",
        F.when(F.col("cost") >= 660, "high").when(F.col("cost") >= 120, "mid").otherwise("low").alias("cost_bucket"),
        "brand",
    )
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.count("*").alias("product_count"),
    )
    .orderBy("first_launch_date")
)
