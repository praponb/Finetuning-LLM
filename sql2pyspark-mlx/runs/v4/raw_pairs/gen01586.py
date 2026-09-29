from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("store_id").isin([3, 6])).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .filter(F.col("p.category").isin(["Grocery", "Home", "Books"]))
    .select(F.col("p.id"), F.col("p.supplier_id"))
)
