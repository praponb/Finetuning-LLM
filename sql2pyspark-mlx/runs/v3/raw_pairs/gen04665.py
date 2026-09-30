from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") < "2024-08-01")
    .select(
        "id",
        "brand",
        "supplier_id",
        "launch_date",
        F.coalesce("product_name", F.lit("N/A")).alias("product_name_filled"),
    )
    .orderBy("brand", "id")
    .limit(5)
)
