from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-04-01", "2024-05-01"))
    .select("product_name", "brand")
    .subtract(
        df_products
        .filter(F.col("category") != "Sports")
        .select("product_name", "brand")
    )
)
