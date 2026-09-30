from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") > 381) & F.month("launch_date").isin([1, 3, 12]))
    .select("tags", "brand", "product_name")
    .orderBy("brand")
)
