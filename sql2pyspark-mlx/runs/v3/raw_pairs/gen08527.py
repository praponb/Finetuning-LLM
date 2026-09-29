from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") < "2024-02-01")
    .select(
        "id",
        "cost",
        "brand",
        "supplier_id",
        F.concat_ws("/", "category", "brand").alias("category_brand_key"),
    )
)
