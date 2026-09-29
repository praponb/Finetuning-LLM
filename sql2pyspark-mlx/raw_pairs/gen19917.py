from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.year("launch_date") == 2024) & F.col("product_name").like("Des%"))
    .select("price", "launch_date", (F.col("price") * F.col("cost")).alias("margin"))
)
