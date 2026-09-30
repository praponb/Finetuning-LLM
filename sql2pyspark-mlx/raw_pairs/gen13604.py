from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select("id", "stock_qty", "category")
    .orderBy(F.col("id").desc(), "stock_qty")
)
