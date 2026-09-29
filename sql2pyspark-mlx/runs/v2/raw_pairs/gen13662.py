from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Sports", "Fashion"]))
    .select("tags", (F.col("stock_qty") + F.col("cost")).alias("stock_qty_plus_cost"))
    .orderBy(F.col("stock_qty_plus_cost").desc())
)
