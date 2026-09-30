from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .filter(F.month(F.col("p.launch_date")).isin([9, 10, 11]))
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.price"))
    .orderBy(F.col("price").desc(), "id")
    .limit(1)
)
