from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("brand") == "Globex")

supplier_id_summary = (
    filtered_products
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = supplier_id_summary.orderBy("first_launch_date")
