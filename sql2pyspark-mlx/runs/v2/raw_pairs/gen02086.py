from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") > 400) & F.month("launch_date").isin([1, 6, 11]))
    .select("tags", "product_name", F.substring("brand", 1, 4).alias("brand_prefix"))
    .orderBy("brand_prefix")
)
