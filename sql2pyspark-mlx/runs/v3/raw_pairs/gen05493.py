from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Stark") & F.col("product_name").like("%Run%"))
    .groupBy("brand", "category")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count")
)
