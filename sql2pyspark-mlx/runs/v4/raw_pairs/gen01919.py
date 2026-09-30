from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("product_name").like("O%") & F.col("brand").isin(["Initech", "Umbrella", "Acme"])) | F.col("product_name").like("N%"))
)

products_stats = (
    recent_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
)

result = products_stats.orderBy(F.col("stock_qty_range").desc())
