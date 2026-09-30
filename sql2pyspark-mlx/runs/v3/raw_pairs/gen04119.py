from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("units").between(28, 39)).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(~F.col("p.category").isin(["Sports", "Books", "Grocery"]))
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.cost"), F.col("p.brand"))
)
