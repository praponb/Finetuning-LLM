from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Noi%") & F.array_contains("tags", "bestseller"))
    .groupBy(
        F.when(F.col("category").isin(["Fashion", "Books"]), "core").otherwise("other").alias("category_group"),
        "supplier_id",
    )
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("category_group").desc(), "supplier_id")
    .limit(3)
)
