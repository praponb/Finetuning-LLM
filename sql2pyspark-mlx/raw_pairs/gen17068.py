from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date") <= "2024-05-20")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
    )
)
