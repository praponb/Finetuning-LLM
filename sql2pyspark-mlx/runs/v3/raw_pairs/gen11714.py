from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.concat(F.col("p.product_name"), F.lit(" - "), F.col("p.brand")).alias("product_name_brand"),
    )
    .orderBy(F.col("quantity").desc(), "id")
    .limit(10)
)
