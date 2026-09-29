from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("p.brand").isin(["Globex", "Umbrella", "Acme"]))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.store_id"), F.col("p.cost"))
)
