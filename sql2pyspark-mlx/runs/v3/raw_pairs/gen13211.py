from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("p.stock_qty") == 303) & F.col("s.city").isNull())
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.substring(F.col("s.city"), 1, 3).alias("city_prefix"),
    )
    .orderBy(F.col("supplier_id").desc())
)
