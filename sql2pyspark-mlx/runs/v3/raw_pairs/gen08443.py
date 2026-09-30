from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .select(
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
    )
    .orderBy(F.col("category").desc())
)
