from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("cost").isNotNull() & F.col("brand").isNotNull() & (F.lower(F.trim("product_name")) == "office chair"))
)

products_stats = (
    recent_products
    .groupBy("brand", "category")
    .agg(F.countDistinct("price").alias("unique_categorys"))
)

result = products_stats.orderBy("brand")
