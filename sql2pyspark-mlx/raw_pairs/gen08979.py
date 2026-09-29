from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.sale_date") <= "2024-12-01")
    .select(F.col("p.stock_qty"), F.col("ss.revenue"), F.col("ss.units"))
    .orderBy(F.col("stock_qty").desc())
)
