from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.launch_date").between("2023-12-01", "2025-05-03") & F.col("p.brand").isin(["Stark", "Acme"]))
    .select(F.col("p.category"), F.col("p.launch_date"), F.col("i.product_id"))
)
