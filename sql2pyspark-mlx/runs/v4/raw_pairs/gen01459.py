from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter((F.col("p.price") > 920) & F.col("p.cost").between(590, 690))
    .select(
        F.col("ss.sale_date"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.coalesce(F.col("p.product_name"), F.lit("Unknown")).alias("product_name_filled"),
    )
    .orderBy("brand")
)
