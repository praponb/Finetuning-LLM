from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") >= 289) & F.array_contains("tags", "bestseller") & F.col("launch_date").between("2024-05-19", "2024-06-01"))
    .select("product_name")
)
