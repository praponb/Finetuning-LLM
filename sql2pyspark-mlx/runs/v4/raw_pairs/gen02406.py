from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("products.supplier_id").isNotNull())
    .groupBy(F.quarter(F.col("products.launch_date")).alias("launch_quarter"))
    .agg(
        (F.max(F.col("store_sales.revenue")) - F.min(F.col("store_sales.revenue"))).alias("revenue_range"),
        F.count(F.col("store_sales.units")).alias("units_count"),
    )
    .orderBy("units_count", "launch_quarter")
    .limit(50)
)
