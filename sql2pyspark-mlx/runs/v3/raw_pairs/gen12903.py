from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("last_restock").between("2023-07-01", "2024-04-11")).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .filter(F.col("p.category") == "Books")
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("p.product_name"))
)
