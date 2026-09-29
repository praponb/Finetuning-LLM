from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.city"))
    .agg(F.min(F.col("products.stock_qty")).alias("min_stock_qty"))
    .filter(F.col("min_stock_qty") >= 390)
    .orderBy("min_stock_qty")
)
