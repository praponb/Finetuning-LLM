from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") >= 220) & F.array_contains("tags", "bestseller"))
    .select("tags", "stock_qty", "category")
    .orderBy("category", "stock_qty")
)
