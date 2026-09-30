from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Chai%"))
    .select("launch_date", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("price") <= 780)
        .select("launch_date", "supplier_id")
    )
    .orderBy("launch_date")
)
