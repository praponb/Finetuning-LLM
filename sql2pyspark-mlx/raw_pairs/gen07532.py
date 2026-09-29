from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.cost").between(290, 520))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        (F.col("ss.revenue") * F.col("ss.units")).alias("revenue_per_unit"),
    )
    .orderBy("revenue_per_unit", "id")
    .limit(10)
)
