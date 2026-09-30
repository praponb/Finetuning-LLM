from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.rating") < 1.8)
    .select(
        F.col("s.supplier_name"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.concat(F.col("s.country"), F.lit(" "), F.col("s.supplier_name")).alias("country_supplier_name"),
    )
)
