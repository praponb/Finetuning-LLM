from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.product_name").like("C%"))
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("s.city"),
        F.substring(F.col("p.brand"), 1, 4).alias("brand_prefix"),
    )
    .orderBy("brand_prefix")
)
