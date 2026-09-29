from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select("id", "stock_qty", "cost")
    .orderBy(F.col("stock_qty").desc(), F.col("cost").desc(), "id")
    .limit(5)
)
