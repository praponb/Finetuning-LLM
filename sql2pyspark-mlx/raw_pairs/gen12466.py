from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("store_id") == 3).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .filter(F.col("p.product_name").like("Ru%"))
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
    )
)
