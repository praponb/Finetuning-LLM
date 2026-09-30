from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        F.add_months("launch_date", 12).alias("launch_date_plus_months"),
        F.concat_ws("-", "category", "product_name").alias("category_product_name_key"),
    )
)
