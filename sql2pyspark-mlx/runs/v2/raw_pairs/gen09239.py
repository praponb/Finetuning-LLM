from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("store_id").isin([1, 2, 4])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
    )
    .orderBy("supplier_id", F.col("stock_qty").desc())
)
