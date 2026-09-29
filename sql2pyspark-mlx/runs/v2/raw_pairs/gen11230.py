from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.city").isNotNull())
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("p.price"),
        F.col("p.brand"),
    )
    .orderBy("brand")
)
