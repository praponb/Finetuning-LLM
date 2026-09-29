from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("price") <= 1080) & F.col("supplier_id").isNotNull()) | (F.col("launch_date") <= "2023-04-21"))
    .select(
        "id",
        "launch_date",
        "category",
        "cost",
        F.trim("category").alias("clean_category"),
    )
)
