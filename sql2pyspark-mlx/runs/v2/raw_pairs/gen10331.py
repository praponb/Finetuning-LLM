from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.price") >= 370)
    .select(
        F.col("s.country"),
        F.col("p.brand"),
        F.col("p.category"),
        F.substring(F.col("s.supplier_name"), 1, 2).alias("supplier_name_prefix"),
    )
)
