from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("quantity") == 153).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .filter(F.col("p.product_name").like("L%"))
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
)
