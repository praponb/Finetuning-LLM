from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("supplier_id").isNull()).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("i.quantity") != 161)
    .select(F.col("i.id"), F.col("i.quantity"))
    .orderBy("id")
)
