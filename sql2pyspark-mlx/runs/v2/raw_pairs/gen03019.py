from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("product_id") == 6).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.brand"), F.col("p.price"))
    .orderBy(F.col("supplier_id").desc(), F.col("price").desc())
)
