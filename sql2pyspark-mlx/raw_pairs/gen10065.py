from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Grocery") & (F.size("tags") >= 2))
    .select("cost", "stock_qty")
    .orderBy(F.col("cost").desc())
)
