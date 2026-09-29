from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Books")
    .select("supplier_id", "stock_qty", "cost")
    .orderBy(F.col("supplier_id").desc())
)
