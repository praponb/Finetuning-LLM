from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("store_id").isin([1, 3, 4, 6])).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .filter(F.col("p.category").isin(["Books", "Sports"]))
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.price"),
    )
)
