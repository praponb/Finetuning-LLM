from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("p.cost"),
        F.trim(F.col("s.supplier_name")).alias("clean_supplier_name"),
    )
)
