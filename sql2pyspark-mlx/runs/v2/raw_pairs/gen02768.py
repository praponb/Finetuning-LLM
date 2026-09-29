from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") > 690) & F.month("launch_date").isin([3, 8, 10]))
    .select("id", "product_name")
)
