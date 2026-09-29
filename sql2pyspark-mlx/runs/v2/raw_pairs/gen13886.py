from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
    )
)
