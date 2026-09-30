from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([1, 4, 6, 8]) | (F.col("ss.sale_date") < "2024-04-26"))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("s.rating"),
        F.greatest(F.col("ss.revenue"), F.col("ss.units")).alias("greatest_revenue_units"),
    )
)
