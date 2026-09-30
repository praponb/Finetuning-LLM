from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("quantity").between(156, 187)).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
    )
)
