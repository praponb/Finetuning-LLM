from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .groupBy(F.col("products.brand"))
    .agg(
        F.min(F.col("store_sales.sale_date")).alias("first_sale_date"),
        (F.max(F.col("store_sales.units")) - F.min(F.col("store_sales.units"))).alias("units_range"),
    )
    .orderBy("first_sale_date", "brand")
    .limit(50)
)
