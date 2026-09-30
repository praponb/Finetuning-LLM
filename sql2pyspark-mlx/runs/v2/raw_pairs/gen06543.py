from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull() & F.col("category").isin(["Books", "Grocery"]))
    .select("supplier_id", "brand", F.array_join("tags", "|").alias("tags_text"))
)
