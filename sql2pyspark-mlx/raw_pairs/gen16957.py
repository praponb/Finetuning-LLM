from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-12-01", "2024-01-01") | (F.col("stock_qty") < 378))
    .select("id", "launch_date", "stock_qty", "product_name")
)
