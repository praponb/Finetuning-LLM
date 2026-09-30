from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Sma%"))
    .select("id", "product_name", "price")
)
