from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(
        F.when(F.col("products.stock_qty") >= 391, "high").when(F.col("products.stock_qty") >= 129, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.countDistinct(F.col("store_sales.units")).alias("unique_products"),
        F.max(F.col("store_sales.sale_date")).alias("last_sale_date"),
    )
)
