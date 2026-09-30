from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("product_id") == 21).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .filter(~F.col("p.category").isin(["Grocery", "Books", "Sports"]))
    .select(F.col("p.id"), F.col("p.cost"))
)
