from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("p.price") < 610)
    .select(
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("ss.units"),
        F.col("ss.product_id"),
    )
)
