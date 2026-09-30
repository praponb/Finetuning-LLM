from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") > 140) & (F.size("tags") > 2))
    .select(
        "id",
        "cost",
        "category",
        F.lower("category").alias("category_lower"),
        F.initcap(F.trim("category")).alias("category_title"),
    )
)
