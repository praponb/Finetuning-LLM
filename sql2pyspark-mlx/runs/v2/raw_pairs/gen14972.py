from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.sort_array(F.col("p.tags")).alias("sorted_tags"),
    )
    .orderBy("category")
)
