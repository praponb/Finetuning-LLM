from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["US", "SG"]))
    .select(
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
        F.col("s.country"),
        F.col("s.rating"),
    )
    .orderBy(F.col("units").desc())
)
