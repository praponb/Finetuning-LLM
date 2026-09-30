from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("s.city") == "Bangkok") & F.col("p.supplier_id").isin([23, 35]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("p.cost"),
        F.col("s.country"),
        F.col("s.city"),
    )
    .orderBy("sale_date", "id")
    .limit(5)
)
