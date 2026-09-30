from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2025-08-01")
    .select(
        "id",
        "supplier_id",
        "tags",
        "product_name",
        F.date_add("launch_date", 23).alias("launch_date_plus_23d"),
    )
)
