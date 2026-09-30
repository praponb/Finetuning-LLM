from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("launch_date")
    .unionByName(
        df_products
        .filter(F.col("launch_date").between("2025-05-01", "2025-06-01"))
        .select("launch_date")
    )
    .orderBy("launch_date")
)
