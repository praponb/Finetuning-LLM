from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Grocery")
    .select(
        "id",
        "supplier_id",
        "product_name",
        "cost",
        F.date_format("launch_date", "MMM yyyy").alias("launch_date_label"),
        F.array_join("tags", ", ").alias("tags_text"),
    )
)
