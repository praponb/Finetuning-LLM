from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.cost").between(220, 370))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.initcap(F.trim(F.col("p.product_name"))).alias("product_name_title"),
    )
    .orderBy(F.col("supplier_name").desc(), "product_name_title", "id")
    .limit(5)
)
