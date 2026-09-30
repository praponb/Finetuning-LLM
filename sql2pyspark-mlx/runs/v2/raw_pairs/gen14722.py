from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .filter(F.array_contains(F.col("products.tags"), "premium") | F.col("store_sales.product_id").isin([1, 6, 10]))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("store_sales.sale_date"),
        F.col("products.price"),
        F.col("products.stock_qty"),
    )
    .orderBy("id")
    .limit(10)
)
