from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(590, 630) & F.col("launch_date").between("2023-04-01", "2024-07-01"))
    .select("id", "cost", "product_name", "brand")
)
