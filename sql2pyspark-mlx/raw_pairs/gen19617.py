from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("p.cost") >= 650) | (F.size(F.col("p.tags")) > 1))
    .select(
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("p.brand"),
        F.col("p.price"),
    )
)
